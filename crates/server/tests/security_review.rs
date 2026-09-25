//! P2-T6 security review: regression tests for its findings (fixed in
//! P2-T6b), plus the properties the review found to hold.
//!
//! Findings covered:
//! - F1: unauthenticated TLS connections could be held forever (no
//!   pre-auth timeout), were invisible (not in `stats`) and unbounded, so
//!   they could exhaust file descriptors and take down every listener,
//!   HTTP included. Fixed by `auth.timeout`, the shared
//!   `server.max_pending_connections` cap, and the
//!   `beanstalkd_pending_connections` / `beanstalkd_pending_rejected_total`
//!   / `beanstalkd_auth_timeouts_total` / `beanstalkd_auth_failures_total`
//!   metrics (also in `/admin` under `"server_rs"`).
//! - F3: `/admin` returned every tube and every request cost an O(tubes)
//!   engine snapshot. Fixed by capping `/admin` at `max_tube_series`,
//!   bounded engine snapshots (`Engine::snapshot_limited`) and a snapshot
//!   cache (`http.snapshot_min_interval`).
//! - F4: the 64-permit HTTP connection pool, taken before accept, let slow
//!   clients starve `/healthz`. Fixed by a larger pool, a 2 s header
//!   timeout, and routing `/healthz` / `/readyz` without waiting on any
//!   other permit.
//!
//! Timeouts in these tests are shortened through the configuration.

#![allow(clippy::unwrap_used)]
#![allow(clippy::too_many_lines)]

mod common;

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use common::p2::{BIN, Certs, ClientCert, ConfigServer, End, Proto, TlsStream, get, stat_of};

const TOKEN: &str = "s3cr3t-Token-4b1d";

// ---------------------------------------------------------------------------
// Shared config templates and helpers
// ---------------------------------------------------------------------------

/// Port 0: TLS + token auth; port 1: HTTP. `extra` is appended (it may
/// add `[server]` keys, `auth.timeout` must go through `auth_timeout`).
fn token_and_http(auth_timeout: &str, extra: &str) -> String {
    format!(
        r#"
[[listener]]
addr = "127.0.0.1:{{port0}}"
tls = true
auth = "token"

[tls]
cert = "server.pem"
key = "server.key"

[auth]
tokens = ["{TOKEN}"]
timeout = "{auth_timeout}"

[http]
addr = "127.0.0.1:{{port1}}"
{extra}
"#
    )
}

fn connect_noauth(server: &ConfigServer, certs: &Certs) -> Proto<TlsStream> {
    server.tls(0, certs.client_config(ClientCert::None))
}

/// The value of the unlabelled sample `name` in `/metrics`.
fn metric(http: SocketAddr, name: &str) -> u64 {
    let body = get(http, "/metrics").body;
    body.lines()
        .find_map(|l| l.strip_prefix(&format!("{name} ")))
        .unwrap_or_else(|| panic!("{name} missing from /metrics"))
        .parse()
        .unwrap()
}

/// Polls `/metrics` until `name` satisfies `ok` (the server-side counters
/// are sampled live, never cached), or fails after 10 s.
fn wait_metric(http: SocketAddr, name: &str, ok: impl Fn(u64) -> bool) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let v = metric(http, name);
        if ok(v) {
            return v;
        }
        assert!(Instant::now() < deadline, "{name} stuck at {v}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

// ===========================================================================
// F1: pre-auth connections time out, are counted, and are capped
// ===========================================================================

/// F1 (the review's "F2" PoC): a client that completes the TLS handshake
/// but never authenticates is closed silently after `auth.timeout`. While
/// it waits it shows as pending in `/metrics` (and `/admin`), not in
/// `stats`, which stays exactly the reference's.
#[test]
fn f1_idle_pre_auth_connections_time_out_and_are_visible() {
    let (server, certs) = ConfigServer::start(&token_and_http("1s", ""), 2, &[]);
    let http = server.addr(1);
    // The readiness probes of `ConfigServer::start` were pending briefly.
    wait_metric(http, "beanstalkd_pending_connections", |v| v == 0);

    const IDLE: usize = 10;
    let mut idle: Vec<_> = (0..IDLE).map(|_| connect_noauth(&server, &certs)).collect();
    assert_eq!(
        wait_metric(http, "beanstalkd_pending_connections", |v| v == IDLE as u64),
        IDLE as u64
    );
    let admin: serde_json::Value = serde_json::from_str(&get(http, "/admin").body).unwrap();
    assert_eq!(admin["server_rs"]["pending-connections"], IDLE);
    assert!(
        admin["server"].get("pending-connections").is_none(),
        "the stats object is unchanged"
    );

    // `stats` does not know about them (it stays byte-compatible).
    let mut ok = connect_noauth(&server, &certs);
    assert_eq!(ok.cmd(&format!("auth {TOKEN}")), "AUTHENTICATED");
    let stats = ok.yaml("stats");
    assert_eq!(stat_of(&stats, "current-connections"), "1");
    assert_eq!(stat_of(&stats, "total-connections"), "1");
    assert!(!stats.contains("pending"), "{stats}");

    // Each idle connection is closed, without a reply, once the timeout
    // has passed.
    let t0 = Instant::now();
    for c in &mut idle {
        let (bytes, end) = c.read_to_end(Duration::from_secs(5));
        assert_eq!(end, End::Closed, "an idle pre-auth connection is closed");
        assert!(bytes.is_empty(), "closed silently: {bytes:?}");
    }
    assert!(t0.elapsed() < Duration::from_secs(4), "{:?}", t0.elapsed());
    assert_eq!(
        wait_metric(http, "beanstalkd_auth_timeouts_total", |v| v == IDLE as u64),
        IDLE as u64
    );
    assert_eq!(
        wait_metric(http, "beanstalkd_pending_connections", |v| v == 0),
        0
    );
    // The authenticated connection was not affected.
    assert_eq!(ok.cmd("use default"), "USING default");
    assert_eq!(metric(http, "beanstalkd_auth_failures_total"), 0);
}

/// F1: `auth.timeout` only applies before authentication.
#[test]
fn f1_authenticated_connections_are_not_timed_out() {
    let (server, certs) = ConfigServer::start(&token_and_http("300ms", ""), 2, &[]);
    let http = server.addr(1);
    let mut c = connect_noauth(&server, &certs);
    assert_eq!(c.cmd(&format!("auth {TOKEN}")), "AUTHENTICATED");
    // Several timeouts' worth of idleness, then more commands.
    std::thread::sleep(Duration::from_millis(1200));
    assert_eq!(c.put(b"still here"), "INSERTED 1");
    std::thread::sleep(Duration::from_millis(600));
    assert_eq!(c.stat("stats", "current-connections"), "1");
    assert_eq!(metric(http, "beanstalkd_auth_timeouts_total"), 0);
    assert_eq!(metric(http, "beanstalkd_pending_connections"), 0);
}

/// F1: once `server.max_pending_connections` connections are pending, new
/// TLS connections are closed at once (counted, and logged at most once
/// a second); the listener recovers as soon as pending ones go away.
/// Authentication failures are counted too.
#[test]
fn f1_pending_cap_rejects_new_tls_connections_and_recovers() {
    let config = token_and_http(
        "30s",
        "[server]\nmax_pending_connections = 3\n\n[log]\nlevel = \"info\"\n",
    );
    let (server, certs) = ConfigServer::start(&config, 2, &[]);
    let http = server.addr(1);
    let tls_addr = server.addr(0);
    let client = certs.client_config(ClientCert::None);
    wait_metric(http, "beanstalkd_pending_connections", |v| v == 0);
    let rejected_before = metric(http, "beanstalkd_pending_rejected_total");

    let mut held: Vec<_> = (0..3).map(|_| connect_noauth(&server, &certs)).collect();
    wait_metric(http, "beanstalkd_pending_connections", |v| v == 3);

    // Full: a new TLS connection is closed before its handshake.
    let t0 = Instant::now();
    assert!(
        common::p2::tls_connect(tls_addr, client.clone()).is_err(),
        "a TLS connection past the cap must be refused"
    );
    // A flood of them is cheap to refuse, and not logged line by line.
    const FLOOD: u64 = 40;
    for _ in 0..FLOOD {
        let mut s = TcpStream::connect_timeout(&tls_addr, Duration::from_secs(2)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
        let mut buf = [0u8; 16];
        // Closed by the server right away (EOF or reset).
        assert!(matches!(s.read(&mut buf), Ok(0) | Err(_)));
    }
    let rejected = wait_metric(http, "beanstalkd_pending_rejected_total", |v| {
        v > rejected_before + FLOOD
    });
    assert_eq!(rejected, rejected_before + FLOOD + 1);
    assert_eq!(metric(http, "beanstalkd_pending_connections"), 3);
    let admin: serde_json::Value = serde_json::from_str(&get(http, "/admin").body).unwrap();
    assert_eq!(admin["server_rs"]["pending-rejected"], rejected);
    let secs = t0.elapsed().as_secs() + 2;
    let log_lines = server
        .stderr()
        .lines()
        .filter(|l| l.contains("too many pending TLS connections"))
        .count() as u64;
    assert!(
        (1..=secs).contains(&log_lines),
        "{log_lines} rejection log lines in ~{secs}s:\n{}",
        server.stderr()
    );

    // A wrong token frees its slot (and counts as a failure).
    let mut bad = held.pop().unwrap();
    assert_eq!(bad.cmd("auth not-the-token"), "UNAUTHORIZED");
    assert_eq!(
        wait_metric(http, "beanstalkd_auth_failures_total", |v| v == 1),
        1
    );
    wait_metric(http, "beanstalkd_pending_connections", |v| v == 2);
    // So does a client that goes away.
    drop(held.pop());
    wait_metric(http, "beanstalkd_pending_connections", |v| v == 1);

    // Recovered: new clients get in and authenticate.
    for _ in 0..2 {
        let mut c = connect_noauth(&server, &certs);
        assert_eq!(c.cmd(&format!("auth {TOKEN}")), "AUTHENTICATED");
        assert_eq!(c.cmd("use default"), "USING default");
    }
    // Authenticated connections are not pending: the one idle connection
    // is all that counts.
    assert_eq!(metric(http, "beanstalkd_pending_connections"), 1);
    drop(held);
}

/// F1: on TLS listeners without token auth (`auth = "none"` and `"mtls"`)
/// a connection is pending only during its handshake; the cap is shared
/// by all TLS listeners.
#[test]
fn f1_handshake_only_listeners_count_pending_during_the_handshake() {
    const CONFIG: &str = r#"
[server]
max_pending_connections = 2

[[listener]]
addr = "127.0.0.1:{port0}"
tls = true

[[listener]]
addr = "127.0.0.1:{port1}"
tls = true
auth = "mtls"

[tls]
cert = "server.pem"
key = "server.key"
client_ca = "ca.pem"

[http]
addr = "127.0.0.1:{port2}"
"#;
    let (server, certs) = ConfigServer::start(CONFIG, 3, &[]);
    let http = server.addr(2);
    wait_metric(http, "beanstalkd_pending_connections", |v| v == 0);

    // Many more established connections than the cap: none is pending.
    let mut open = Vec::new();
    for i in 0..4 {
        let mut t = server.tls(0, certs.client_config(ClientCert::None));
        assert_eq!(t.cmd("use default"), "USING default");
        let mut m = server.tls(1, certs.client_config(ClientCert::Valid));
        assert_eq!(m.cmd("use default"), "USING default", "mtls #{i}");
        open.push(t);
        open.push(m);
    }
    assert_eq!(metric(http, "beanstalkd_pending_connections"), 0);

    // Two connections that never start their handshake fill the shared
    // cap...
    let raw: Vec<_> = (0..2)
        .map(|_| TcpStream::connect_timeout(&server.addr(0), Duration::from_secs(2)).unwrap())
        .collect();
    wait_metric(http, "beanstalkd_pending_connections", |v| v == 2);
    // ... so the mTLS listener refuses new connections too.
    let refused = common::p2::tls_connect(server.addr(1), certs.client_config(ClientCert::Valid))
        .and_then(|mut c| {
            // TLS 1.3: a refusal may only show on the first read.
            c.send(b"use default\r\n");
            let (bytes, end) = c.read_to_end(Duration::from_secs(3));
            if bytes.is_empty() && end != End::Timeout {
                Err(std::io::Error::other("closed"))
            } else {
                Ok(bytes)
            }
        });
    assert!(refused.is_err(), "{refused:?}");
    assert!(metric(http, "beanstalkd_pending_rejected_total") >= 1);

    // Once they go away, both listeners accept again.
    drop(raw);
    wait_metric(http, "beanstalkd_pending_connections", |v| v == 0);
    let mut m = server.tls(1, certs.client_config(ClientCert::Valid));
    assert_eq!(m.cmd("use default"), "USING default");
    let mut t = server.tls(0, certs.client_config(ClientCert::None));
    assert_eq!(t.cmd("use default"), "USING default");
    // The established connections were never affected.
    for c in &mut open {
        assert_eq!(c.cmd("use default"), "USING default");
    }
}

// F1 (the original PoC): with a small RLIMIT_NOFILE, unauthenticated TLS
// connections used to fill the descriptor table, after which no listener
// -- HTTP included -- could accept anything. With the pending cap below
// the descriptor limit, /healthz keeps answering.

struct LimitedServer {
    child: Child,
    dir: tempfile::TempDir,
    tls_port: u16,
    http_port: u16,
    certs: Certs,
}

impl LimitedServer {
    /// Starts the binary under `ulimit -n nofile` (hard and soft).
    fn start(nofile: u32, max_pending: usize) -> LimitedServer {
        let dir = tempfile::tempdir().unwrap();
        let certs = Certs::generate(dir.path());
        let tls_port = common::p2::claim_port();
        let http_port = common::p2::claim_port();
        let cfg = format!(
            r#"
[server]
max_pending_connections = {max_pending}

[[listener]]
addr = "127.0.0.1:{tls_port}"
tls = true
auth = "token"

[tls]
cert = "server.pem"
key = "server.key"

[auth]
tokens = ["{TOKEN}"]
timeout = "30s"

[http]
addr = "127.0.0.1:{http_port}"

[log]
level = "error"
"#
        );
        let cfg_path = dir.path().join("config.toml");
        std::fs::write(&cfg_path, cfg).unwrap();
        // `ulimit -n N` in bash sets BOTH the soft and hard limit, so the
        // server's raise_nofile_limit() cannot lift it back up.
        let script = format!("ulimit -n {nofile}; exec \"$@\"");
        let child = Command::new("/bin/bash")
            .arg("-c")
            .arg(&script)
            .arg("sh")
            .arg(BIN)
            .arg("--config")
            .arg(&cfg_path)
            .current_dir(dir.path())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let s = LimitedServer {
            child,
            dir,
            tls_port,
            http_port,
            certs,
        };
        // Wait until both ports listen.
        let deadline = Instant::now() + Duration::from_secs(20);
        for port in [s.tls_port, s.http_port] {
            let addr: SocketAddr = ([127, 0, 0, 1], port).into();
            while TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_err() {
                assert!(Instant::now() < deadline, "server never listened");
                std::thread::sleep(Duration::from_millis(20));
            }
        }
        s
    }
}

impl Drop for LimitedServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        common::p2::release(self.tls_port);
        common::p2::release(self.http_port);
        let _ = &self.dir;
    }
}

#[test]
fn f1_pre_auth_connections_cannot_exhaust_descriptors() {
    const NOFILE: u32 = 96;
    const MAX_PENDING: usize = 32;
    let server = LimitedServer::start(NOFILE, MAX_PENDING);
    let tls_addr: SocketAddr = ([127, 0, 0, 1], server.tls_port).into();
    let http_addr: SocketAddr = ([127, 0, 0, 1], server.http_port).into();

    let (body, _) = raw_http(http_addr, "/healthz", Duration::from_secs(2))
        .expect("healthz before the fd pressure");
    assert!(body.contains("200"), "baseline /healthz: {body:?}");

    // Try to hold more unauthenticated connections than there are
    // descriptors: only MAX_PENDING get through their handshake.
    let client_cfg = server.certs.client_config(ClientCert::None);
    let mut tls_held = Vec::new();
    let mut refused = 0;
    for _ in 0..(NOFILE as usize) {
        match common::p2::tls_connect(tls_addr, client_cfg.clone()) {
            Ok(p) => tls_held.push(p),
            Err(_) => refused += 1,
        }
    }
    // Plus raw TCP connections that never start a handshake.
    let mut held = Vec::new();
    for _ in 0..64 {
        if let Ok(s) = TcpStream::connect_timeout(&tls_addr, Duration::from_millis(100)) {
            held.push(s);
        }
    }
    assert!(
        tls_held.len() <= MAX_PENDING,
        "{} pre-auth connections held",
        tls_held.len()
    );
    assert!(refused > 0);
    std::thread::sleep(Duration::from_millis(500));

    // /healthz still answers, and so does /metrics, which tells why.
    let healthz = raw_http(http_addr, "/healthz", Duration::from_secs(2));
    let healthz_ok = healthz.as_ref().is_some_and(|(b, _)| b.contains("200 OK"));
    assert!(
        healthz_ok,
        "/healthz under descriptor pressure: {healthz:?}"
    );
    assert!(metric(http_addr, "beanstalkd_pending_rejected_total") >= refused);
    assert!(metric(http_addr, "beanstalkd_pending_connections") <= MAX_PENDING as u64);

    // And an authenticated client still gets in once some pending ones
    // are gone.
    tls_held.truncate(MAX_PENDING / 2);
    drop(held);
    wait_metric(http_addr, "beanstalkd_pending_connections", |v| {
        v <= (MAX_PENDING / 2) as u64
    });
    let mut c = common::p2::tls_connect(tls_addr, client_cfg).expect("room again");
    assert_eq!(c.cmd(&format!("auth {TOKEN}")), "AUTHENTICATED");
    assert_eq!(c.put(b"x"), "INSERTED 1");
    drop(tls_held);
}

// ===========================================================================
// F3: /admin is capped, and snapshots are bounded and cached
// ===========================================================================

#[test]
fn f3_admin_and_snapshot_are_bounded_by_max_tube_series() {
    const CONFIG: &str = r#"
[[listener]]
addr = "127.0.0.1:{port0}"

[http]
addr = "127.0.0.1:{port1}"
max_tube_series = 5
"#;
    let server = ConfigServer::start(CONFIG, 2, &[]).0;
    let http_addr = server.addr(1);

    let mut c = server.plain(0);
    const TUBES: usize = 300;
    for i in 0..TUBES {
        assert_eq!(c.cmd(&format!("use tube-{i}")), format!("USING tube-{i}"));
        assert_eq!(c.put(b"x"), format!("INSERTED {}", i + 1));
    }

    // /metrics: 5 tubes * 5 states, flagged as truncated.
    let metrics = get(http_addr, "/metrics").body;
    let series = metrics
        .lines()
        .filter(|l| l.starts_with("beanstalkd_tube_current_jobs{"))
        .count();
    assert_eq!(series, 25);
    assert!(metrics.contains("\nbeanstalkd_tube_series_truncated 1\n"));
    // The full count is still there.
    assert!(metrics.contains("\nbeanstalkd_current_tubes 301\n"));

    // /admin: the first 5 tubes in list order, flagged as truncated.
    let admin = get(http_addr, "/admin").body;
    let doc: serde_json::Value = serde_json::from_str(&admin).unwrap();
    let names: Vec<&str> = doc["tubes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["default", "tube-0", "tube-1", "tube-2", "tube-3"]);
    assert_eq!(doc["tubes_truncated"], true);
    assert_eq!(doc["tube_limit"], 5);
    assert_eq!(doc["server"]["current-tubes"], 301);
    assert!(admin.len() < 8192, "{} bytes", admin.len());
}

/// F3: `/metrics` and `/admin` share one snapshot for
/// `http.snapshot_min_interval` (an hour here, so this does not depend on
/// timing): state changes made after it are not visible until it expires.
/// (The single engine snapshot per interval is counted in `http`'s unit
/// tests.)
#[test]
fn f3_snapshots_are_reused_within_snapshot_min_interval() {
    const CONFIG: &str = r#"
[[listener]]
addr = "127.0.0.1:{port0}"

[http]
addr = "127.0.0.1:{port1}"
snapshot_min_interval = "3600s"
"#;
    let server = ConfigServer::start(CONFIG, 2, &[]).0;
    let http_addr = server.addr(1);
    let mut c = server.plain(0);
    assert_eq!(c.put(b"a"), "INSERTED 1");

    let first = get(http_addr, "/metrics").body;
    assert!(first.contains("\nbeanstalkd_jobs_total 1\n"), "{first}");
    assert_eq!(c.put(b"b"), "INSERTED 2");
    assert_eq!(c.stat("stats", "total-jobs"), "2");
    for _ in 0..5 {
        let again = get(http_addr, "/metrics").body;
        assert!(
            again.contains("\nbeanstalkd_jobs_total 1\n"),
            "cached: {again}"
        );
        let admin: serde_json::Value =
            serde_json::from_str(&get(http_addr, "/admin").body).unwrap();
        assert_eq!(admin["server"]["total-jobs"], 1, "cached");
    }
}

// ===========================================================================
// F4: slow HTTP clients no longer starve /healthz
// ===========================================================================

#[test]
fn f4_slow_http_clients_do_not_starve_healthz() {
    const CONFIG: &str = r#"
[[listener]]
addr = "127.0.0.1:{port0}"

[http]
addr = "127.0.0.1:{port1}"
"#;
    let server = ConfigServer::start(CONFIG, 2, &[]).0;
    let http_addr = server.addr(1);

    for held in [64, 200] {
        // Connections that never finish their headers.
        let mut slow = Vec::new();
        for _ in 0..held {
            let mut s = TcpStream::connect_timeout(&http_addr, Duration::from_millis(500)).unwrap();
            s.write_all(b"GET /metrics HTTP/1.1\r\nHost: x\r\n")
                .unwrap();
            slow.push(s);
        }
        std::thread::sleep(Duration::from_millis(200));

        for path in ["/healthz", "/readyz"] {
            let t0 = Instant::now();
            let resp = raw_http(http_addr, path, Duration::from_millis(1500));
            let elapsed = t0.elapsed();
            assert!(
                resp.as_ref().is_some_and(|(b, _)| b.contains("200 OK")),
                "{path} with {held} slow clients: {resp:?} in {elapsed:?}"
            );
            assert!(elapsed < Duration::from_millis(1500), "{elapsed:?}");
        }
        // Monitoring still works alongside them.
        let (metrics, _) =
            raw_http(http_addr, "/metrics", Duration::from_millis(1500)).expect("metrics");
        assert!(metrics.contains("200 OK"), "{metrics}");
        drop(slow);
    }
}

// ===========================================================================
// Properties that HOLD (regression tests)
// ===========================================================================

/// HOLD: before authentication, a `put` header is refused *at the header*
/// (Frame::PutStarted), so the body is never read/buffered. An attacker
/// cannot make the server buffer a large body before authenticating.
#[test]
fn hold_pre_auth_put_body_is_not_buffered() {
    let (server, certs) = ConfigServer::start(&token_and_http("10s", ""), 2, &[]);
    let mut c = connect_noauth(&server, &certs);

    // Announce a large body but send none of it.
    c.send(b"put 0 0 60 60000\r\n");
    let (bytes, end) = c.read_to_end(Duration::from_secs(3));
    assert_eq!(
        String::from_utf8_lossy(&bytes),
        "UNAUTHORIZED\r\n",
        "a put before auth must be refused at the header"
    );
    assert_eq!(end, End::Closed, "and the connection closed");
    drop(server);
}

/// HOLD: an overlong line (no CRLF within LINE_BUF_SIZE) before auth is
/// refused, and pre-auth buffering stays bounded (the codec drains the
/// window rather than growing the buffer without bound).
#[test]
fn hold_pre_auth_overlong_line_is_refused() {
    let (server, certs) = ConfigServer::start(&token_and_http("10s", ""), 2, &[]);
    let mut c = connect_noauth(&server, &certs);
    // 4 KiB with no CRLF: far over the 224-byte line window.
    c.send(&vec![b'A'; 4096]);
    // Now terminate a line so the decoder emits its verdict.
    c.send(b"\r\n");
    let (bytes, end) = c.read_to_end(Duration::from_secs(3));
    let text = String::from_utf8_lossy(&bytes);
    println!("hold_overlong: reply={text:?} end={end:?}");
    assert!(
        text.contains("UNAUTHORIZED") || text.contains("BAD_FORMAT"),
        "overlong line before auth: {text:?}"
    );
    assert_eq!(end, End::Closed);
    drop(server);
}

// ---------------------------------------------------------------------------
// Minimal HTTP helper with a hard deadline (unlike common::p2::http, which
// reads to EOF with a long per-read timeout).
// ---------------------------------------------------------------------------

fn raw_http(addr: SocketAddr, path: &str, deadline: Duration) -> Option<(String, Duration)> {
    let t0 = Instant::now();
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_millis(500)).ok()?;
    s.set_read_timeout(Some(deadline)).ok()?;
    s.set_write_timeout(Some(deadline)).ok()?;
    s.write_all(format!("GET {path} HTTP/1.1\r\nHost: x\r\n\r\n").as_bytes())
        .ok()?;
    let mut buf = [0u8; 4096];
    let mut out = String::new();
    loop {
        if t0.elapsed() > deadline {
            break;
        }
        match s.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                out.push_str(&String::from_utf8_lossy(&buf[..n]));
                if out.contains("\r\n\r\n") {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    Some((out, t0.elapsed()))
}
