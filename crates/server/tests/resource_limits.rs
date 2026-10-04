//! P5-T6 resource limits: what a client that holds descriptors, never reads,
//! or sends endless input can make the server spend. Only what the P2
//! security review (`security_review.rs`) does not already cover; the
//! inventory is in docs/DESIGN.md §6.3.
//!
//! Memory bounds are generous on purpose (they separate "bounded" from
//! "grows with the input", not tune an allocator); the inputs are several
//! times larger than the bounds.

#![allow(clippy::unwrap_used)]

mod common;

use std::io::{BufRead, BufReader, ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use common::Server;

const MIB: usize = 1 << 20;

/// Resident set size of `pid` in KiB.
fn rss_kib(pid: u32) -> u64 {
    if let Ok(s) = std::fs::read_to_string(format!("/proc/{pid}/status")) {
        let line = s.lines().find(|l| l.starts_with("VmRSS:")).unwrap();
        return line.split_whitespace().nth(1).unwrap().parse().unwrap();
    }
    let out = Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout).trim().parse().unwrap()
}

/// CPU time (user + system) `pid` has used.
fn cpu_time(pid: u32) -> Duration {
    if let Ok(s) = std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        // Fields after the command name, which is parenthesized and may
        // contain spaces; utime and stime are fields 14 and 15 (1-based).
        let rest = &s[s.rfind(')').unwrap() + 2..];
        let f: Vec<u64> = rest
            .split_whitespace()
            .skip(11)
            .take(2)
            .map(|x| x.parse().unwrap())
            .collect();
        // USER_HZ is 100 on every Linux the CI and the container use.
        return Duration::from_millis((f[0] + f[1]) * 10);
    }
    let out = Command::new("ps")
        .args(["-o", "cputime=", "-p", &pid.to_string()])
        .output()
        .unwrap();
    // `[[hh:]mm:]ss.cc`
    let s = String::from_utf8_lossy(&out.stdout).trim().to_owned();
    let secs = s
        .split(':')
        .fold(0.0, |acc, part| acc * 60.0 + part.parse::<f64>().unwrap());
    Duration::from_secs_f64(secs)
}

fn stats_ok(server: &Server) {
    let mut c = server.connect();
    assert!(c.cmd("stats").starts_with("OK "));
}

/// A plaintext server under `ulimit -n nofile` (soft and hard, so the
/// server's own raise of the soft limit cannot lift it), counting the
/// lines it logs.
struct LimitedServer {
    child: Child,
    addr: SocketAddr,
    port: u16,
    log_lines: Arc<AtomicUsize>,
}

impl LimitedServer {
    fn start(nofile: u32) -> LimitedServer {
        let port = common::p2::claim_port();
        let script = format!("ulimit -n {nofile}; exec \"$@\"");
        let mut child = Command::new("/bin/bash")
            .arg("-c")
            .arg(&script)
            .arg("sh")
            .arg(common::p2::BIN)
            .args(["-l", "127.0.0.1", "-p", &port.to_string()])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let log_lines = Arc::new(AtomicUsize::new(0));
        let stderr = child.stderr.take().unwrap();
        let counter = log_lines.clone();
        std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines() {
                if line.is_err() {
                    break;
                }
                counter.fetch_add(1, Ordering::Relaxed);
            }
        });
        let addr: SocketAddr = ([127, 0, 0, 1], port).into();
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Ok(mut s) = TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
                s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
                if s.write_all(b"stats\r\n").is_ok() {
                    let mut b = [0u8; 2];
                    if s.read_exact(&mut b).is_ok() && &b == b"OK" {
                        break;
                    }
                }
            }
            assert!(Instant::now() < deadline, "server never answered");
            std::thread::sleep(Duration::from_millis(20));
        }
        LimitedServer {
            child,
            addr,
            port,
            log_lines,
        }
    }

    fn pid(&self) -> u32 {
        self.child.id()
    }

    /// `put` round trip on a fresh connection, if one can be made within
    /// `wait`.
    fn try_put(&self, wait: Duration) -> Option<String> {
        let mut s = TcpStream::connect_timeout(&self.addr, wait).ok()?;
        s.set_read_timeout(Some(wait)).ok()?;
        s.write_all(b"put 0 0 10 1\r\nx\r\n").ok()?;
        let mut line = Vec::new();
        let mut b = [0u8; 1];
        while !line.ends_with(b"\r\n") {
            match s.read(&mut b) {
                Ok(1) => line.push(b[0]),
                _ => return None,
            }
        }
        Some(String::from_utf8_lossy(&line).into_owned())
    }
}

impl Drop for LimitedServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        common::p2::release(self.port);
    }
}

/// With every descriptor in use, connections queue in the listen backlog
/// and `accept` keeps failing (EMFILE). The accept loop must back off
/// instead of retrying (and logging) in a tight loop, and accept again once
/// descriptors are free.
#[test]
fn accept_backs_off_while_descriptors_are_exhausted() {
    const NOFILE: u32 = 64;
    let server = LimitedServer::start(NOFILE);

    let mut held = Vec::new();
    for _ in 0..(NOFILE as usize + 16) {
        held.push(TcpStream::connect_timeout(&server.addr, Duration::from_secs(1)).unwrap());
    }
    // Give the server time to accept up to its limit and hit EMFILE.
    std::thread::sleep(Duration::from_millis(500));
    let lines0 = server.log_lines.load(Ordering::Relaxed);
    let cpu0 = cpu_time(server.pid());
    std::thread::sleep(Duration::from_secs(2));
    let lines = server.log_lines.load(Ordering::Relaxed) - lines0;
    let cpu = cpu_time(server.pid()) - cpu0;
    println!("while exhausted for 2 s: {lines} log lines, {cpu:?} CPU");
    assert!(lines <= 100, "{lines} log lines in 2 s");
    assert!(cpu < Duration::from_millis(500), "{cpu:?} CPU in 2 s");

    drop(held);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(reply) = server.try_put(Duration::from_secs(1)) {
            assert!(reply.starts_with("INSERTED "), "{reply:?}");
            break;
        }
        assert!(Instant::now() < deadline, "no recovery after closing");
    }
}

/// A client that pipelines commands and never reads its replies is held
/// back by TCP: the server stops reading while a reply is unwritten, so
/// what it buffers is bounded by the socket buffers, not by what the client
/// tries to send. Other clients are unaffected.
#[test]
fn a_client_that_never_reads_is_held_by_back_pressure() {
    let server = Server::start(&[]);
    let mut c = server.connect();
    let body = vec![b'b'; 60_000];
    assert!(c.put(0, 0, 60, &body).starts_with("INSERTED "));
    let rss0 = rss_kib(server.pid());

    // Each 8-byte `peek 1` asks for a 60 KB reply.
    let mut s = TcpStream::connect(server.addr).unwrap();
    s.set_write_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    let chunk = b"peek 1\r\n".repeat(8192);
    let mut sent = 0usize;
    let limit = 256 * MIB;
    while sent < limit {
        match s.write(&chunk) {
            Ok(n) => sent += n,
            Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => break,
            Err(e) => panic!("write: {e}"),
        }
    }
    let rss = rss_kib(server.pid());
    println!(
        "slow reader: {} KiB of commands accepted (asking for {} MiB of replies); \
         server RSS {rss0} -> {rss} KiB",
        sent / 1024,
        sent / 8 * 60_000 / MIB
    );
    // What was accepted sits in the kernel's socket buffers (macOS grows
    // loopback buffers to tens of MiB); the server's own memory is the bound
    // that matters.
    assert!(
        sent < limit,
        "{sent} bytes accepted from a client that never reads"
    );
    assert!(rss < rss0 + 64 * 1024, "RSS {rss0} -> {rss} KiB");
    stats_ok(&server);
    let mut other = server.connect();
    assert!(other.put(0, 0, 60, b"y").starts_with("INSERTED "));
}

/// A line that never ends is discarded in `LINE_BUF_SIZE` windows as it
/// arrives (prot.c's `STATE_WANT_ENDLINE`); its end is then `BAD_FORMAT`.
#[test]
fn an_endless_line_is_discarded_as_it_arrives() {
    let server = Server::start(&[]);
    let rss0 = rss_kib(server.pid());
    let mut c = server.connect();
    c.stream
        .set_write_timeout(Some(Duration::from_secs(20)))
        .unwrap();
    let chunk = vec![b'a'; MIB];
    for _ in 0..128 {
        c.send(&chunk);
    }
    let rss = rss_kib(server.pid());
    stats_ok(&server);
    c.send(b"\r\n");
    assert_eq!(c.read_line(), "BAD_FORMAT\r\n");
    assert!(c.cmd("stats").starts_with("OK "));
    println!("128 MiB line: server RSS {rss0} -> {rss} KiB");
    assert!(rss < rss0 + 32 * 1024, "RSS {rss0} -> {rss} KiB");
}

/// A body over `-z` is discarded as it arrives (`STATE_BITBUCKET`), never
/// buffered, and answered with `JOB_TOO_BIG` once it is complete.
#[test]
fn an_oversized_put_body_is_discarded_as_it_arrives() {
    let server = Server::start(&["-z", "1024"]);
    let rss0 = rss_kib(server.pid());
    let mut c = server.connect();
    c.stream
        .set_write_timeout(Some(Duration::from_secs(20)))
        .unwrap();
    let size = 128 * MIB;
    c.send(format!("put 0 0 10 {size}\r\n").as_bytes());
    let chunk = vec![b'z'; MIB];
    for _ in 0..size / MIB {
        c.send(&chunk);
    }
    let rss = rss_kib(server.pid());
    assert!(
        !c.recv_something(Duration::from_millis(200)),
        "reply before the body ended"
    );
    c.send(b"\r\n");
    assert_eq!(c.read_line(), "JOB_TOO_BIG\r\n");
    assert!(c.put(0, 0, 10, b"ok").starts_with("INSERTED "));
    println!("128 MiB oversized body: server RSS {rss0} -> {rss} KiB");
    assert!(rss < rss0 + 32 * 1024, "RSS {rss0} -> {rss} KiB");
}

/// Many connections blocked in `reserve`, then as many jobs: each waiter
/// gets exactly one, and the server stays responsive throughout.
#[test]
fn a_reserve_storm_hands_each_waiter_one_job() {
    const N: usize = 150;
    let server = Server::start(&[]);
    let mut waiters: Vec<_> = (0..N).map(|_| server.connect()).collect();
    for w in &mut waiters {
        w.send(b"reserve-with-timeout 30\r\n");
    }
    let mut producer = server.connect();
    let deadline = Instant::now() + Duration::from_secs(10);
    while producer.stat("stats", "current-waiting") != N.to_string() {
        assert!(Instant::now() < deadline, "waiters never all registered");
        std::thread::sleep(Duration::from_millis(20));
    }
    for i in 0..N {
        assert!(
            producer
                .put(0, 0, 60, format!("{i}").as_bytes())
                .starts_with("INSERTED ")
        );
    }
    let mut got: Vec<usize> = waiters
        .iter_mut()
        .map(|w| {
            let (header, body) = w.read_body_reply();
            assert!(header.starts_with("RESERVED "), "{header:?}");
            String::from_utf8(body).unwrap().parse().unwrap()
        })
        .collect();
    got.sort_unstable();
    assert_eq!(got, (0..N).collect::<Vec<_>>());
    assert_eq!(
        producer.stat("stats", "current-jobs-reserved"),
        N.to_string()
    );
}
