//! Black-box integration tests for the `beanstalkd-rs` binary: spawn the
//! real binary and drive it over raw TCP, exactly as a client would. Per
//! T4 scope, these only touch the `bstk-server` crate's public artifact
//! (the compiled binary); they know nothing about its internals.

#![allow(clippy::unwrap_used)]

mod common;

use std::io::Read;
use std::time::{Duration, Instant};

use common::Server;

// ---------------------------------------------------------------------
// basic flow
// ---------------------------------------------------------------------

#[test]
fn basic_put_reserve_delete_and_stats() {
    let server = Server::start(&[]);
    let mut c = server.connect();

    c.send(b"put 0 0 60 5\r\nhello\r\n");
    assert_eq!(c.read_line(), "INSERTED 1\r\n");

    c.send(b"reserve\r\n");
    let (header, body) = c.read_body_reply();
    assert_eq!(header, "RESERVED 1 5\r\n");
    assert_eq!(body, b"hello");

    c.send(b"delete 1\r\n");
    assert_eq!(c.read_line(), "DELETED\r\n");

    c.send(b"stats\r\n");
    let (header, body) = c.read_body_reply();
    assert!(
        header.starts_with("OK "),
        "unexpected stats header: {header:?}"
    );
    let body = String::from_utf8_lossy(&body);
    assert!(body.contains("current-connections: 1"), "body={body}");
    assert!(body.contains("total-jobs: 1"), "body={body}");
}

#[test]
fn quit_closes_connection() {
    let server = Server::start(&[]);
    let mut c = server.connect();
    c.send(b"quit\r\n");
    let mut buf = [0u8; 16];
    let n = c.stream.read(&mut buf).expect("read after quit");
    assert_eq!(n, 0, "quit must close with no reply");
}

// ---------------------------------------------------------------------
// blocking reserve, disconnect, half-close
// ---------------------------------------------------------------------

#[test]
fn blocking_reserve_woken_by_put_on_another_connection() {
    let server = Server::start(&[]);
    let mut reserver = server.connect();
    let mut putter = server.connect();

    reserver.send(b"reserve\r\n");
    // No job exists yet; confirm we are genuinely blocked, not fast-failing.
    assert!(
        !reserver.recv_something(Duration::from_millis(300)),
        "reserve resolved before any job existed"
    );

    putter.send(b"put 0 0 60 5\r\nhello\r\n");
    assert_eq!(putter.read_line(), "INSERTED 1\r\n");

    let (header, body) = reserver.read_body_reply();
    assert_eq!(header, "RESERVED 1 5\r\n");
    assert_eq!(body, b"hello");
}

#[test]
fn disconnect_of_reserver_releases_job_to_another_waiter() {
    let server = Server::start(&[]);
    let mut putter = server.connect();
    putter.send(b"put 0 0 60 5\r\nhello\r\n");
    assert_eq!(putter.read_line(), "INSERTED 1\r\n");

    let mut reserver1 = server.connect();
    reserver1.send(b"reserve\r\n");
    let (header, body) = reserver1.read_body_reply();
    assert_eq!(header, "RESERVED 1 5\r\n");
    assert_eq!(body, b"hello");

    let mut reserver2 = server.connect();
    reserver2.send(b"reserve\r\n");
    assert!(!reserver2.recv_something(Duration::from_millis(200)));

    // Dropping reserver1 closes its socket; the engine must release job 1
    // back to ready, and reserver2 (already waiting) must pick it up.
    drop(reserver1);

    let (header, body) = reserver2.read_body_reply();
    assert_eq!(header, "RESERVED 1 5\r\n");
    assert_eq!(body, b"hello");
}

#[test]
fn half_close_while_waiting_on_reserve_times_out() {
    let server = Server::start(&[]);
    let mut c = server.connect();
    c.send(b"reserve\r\n");
    assert!(!c.recv_something(Duration::from_millis(200)));

    c.shutdown_write();

    assert_eq!(c.read_line(), "TIMED_OUT\r\n");
}

// ---------------------------------------------------------------------
// pipelining
// ---------------------------------------------------------------------

#[test]
fn pipelined_commands_in_one_write() {
    let server = Server::start(&[]);
    let mut c = server.connect();

    c.send(b"use foo\r\nput 0 0 60 3\r\nbar\r\nstats-tube foo\r\n");

    assert_eq!(c.read_line(), "USING foo\r\n");
    assert_eq!(c.read_line(), "INSERTED 1\r\n");
    let (header, body) = c.read_body_reply();
    assert!(
        header.starts_with("OK "),
        "unexpected stats-tube header: {header:?}"
    );
    let body = String::from_utf8_lossy(&body);
    assert!(body.contains("name: \"foo\""), "body={body}");
}

#[test]
fn put_body_split_across_writes() {
    let server = Server::start(&[]);
    let mut c = server.connect();

    // Split the put line and its body across several small writes,
    // including mid-body, to exercise the codec's `STATE_WANT_DATA`
    // buffering across separate `read()`s.
    c.send(b"put 5 0 6");
    std::thread::sleep(Duration::from_millis(20));
    c.send(b"0 5\r\nhel");
    std::thread::sleep(Duration::from_millis(20));
    c.send(b"lo\r\n");

    assert_eq!(c.read_line(), "INSERTED 1\r\n");

    c.send(b"reserve\r\n");
    let (header, body) = c.read_body_reply();
    assert_eq!(header, "RESERVED 1 5\r\n");
    assert_eq!(body, b"hello");
}

// ---------------------------------------------------------------------
// timing: reserve-with-timeout and TTR expiry
// ---------------------------------------------------------------------

/// prot.c counts a put and allocates its job id as soon as the command
/// line parses (`Frame::PutStarted`), before the body arrives: a put
/// completed meanwhile on another connection gets the *next* id, and
/// another connection's stats already show the pending put.
#[test]
fn put_header_allocates_job_id_before_body_arrives() {
    let server = Server::start(&[]);
    let mut slow = server.connect();
    let mut fast = server.connect();

    slow.send(b"put 0 0 60 5\r\nhe");
    std::thread::sleep(Duration::from_millis(200));

    fast.send(b"stats\r\n");
    let (_, body) = fast.read_body_reply();
    let body = String::from_utf8_lossy(&body);
    assert!(body.contains("\ncmd-put: 1\n"), "body={body}");
    assert!(body.contains("\ncurrent-producers: 1\n"), "body={body}");

    fast.send(b"put 0 0 60 1\r\nx\r\n");
    assert_eq!(fast.read_line(), "INSERTED 2\r\n");
    slow.send(b"llo\r\n");
    assert_eq!(slow.read_line(), "INSERTED 1\r\n");

    // A put abandoned mid-body has still consumed its id.
    let mut quitter = server.connect();
    quitter.send(b"put 0 0 60 5\r\nab");
    std::thread::sleep(Duration::from_millis(100));
    drop(quitter);
    std::thread::sleep(Duration::from_millis(200));
    fast.send(b"put 0 0 60 1\r\ny\r\n");
    assert_eq!(fast.read_line(), "INSERTED 4\r\n");
}

/// `-z -1` is accepted like the reference's `sscanf("%zu")`: it wraps to
/// SIZE_MAX and is clamped to 1 GiB.
#[test]
fn negative_max_job_size_is_clamped_like_the_reference() {
    let server = Server::start(&["-z", "-1"]);
    let mut c = server.connect();
    c.send(b"stats\r\n");
    let (_, body) = c.read_body_reply();
    let body = String::from_utf8_lossy(&body);
    assert!(body.contains("\nmax-job-size: 1073741824\n"), "body={body}");
}

#[test]
fn reserve_with_timeout_times_out_after_about_one_second() {
    let server = Server::start(&[]);
    let mut c = server.connect();

    let start = Instant::now();
    c.send(b"reserve-with-timeout 1\r\n");
    let reply = c.read_line();
    let elapsed = start.elapsed();

    assert_eq!(reply, "TIMED_OUT\r\n");
    assert!(
        elapsed >= Duration::from_millis(800) && elapsed <= Duration::from_millis(2500),
        "elapsed={elapsed:?}"
    );
}

#[test]
fn ttr_expiry_returns_job_to_ready_and_bumps_timeouts() {
    let server = Server::start(&[]);
    let mut reserver = server.connect();
    // ttr = 1s: short enough to expire quickly and exercise TTR expiry
    // rather than DEADLINE_SOON/reserve semantics.
    reserver.send(b"put 0 0 1 5\r\nhello\r\n");
    assert_eq!(reserver.read_line(), "INSERTED 1\r\n");

    reserver.send(b"reserve\r\n");
    let (header, body) = reserver.read_body_reply();
    assert_eq!(header, "RESERVED 1 5\r\n");
    assert_eq!(body, b"hello");

    // Do not touch/delete/release it: let the 1s TTR expire. A second
    // connection blocked in reserve must pick the job back up once it
    // returns to ready.
    let mut other = server.connect();
    other
        .stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set_read_timeout");
    other.send(b"reserve\r\n");
    let (header, body) = other.read_body_reply();
    assert_eq!(header, "RESERVED 1 5\r\n");
    assert_eq!(body, b"hello");

    other.send(b"stats-job 1\r\n");
    let (_, body) = other.read_body_reply();
    let body = String::from_utf8_lossy(&body);
    assert!(body.contains("timeouts: 1"), "body={body}");
}

// ---------------------------------------------------------------------
// signals
// ---------------------------------------------------------------------

#[test]
fn sigusr1_puts_replies_draining() {
    let server = Server::start(&[]);
    let mut c = server.connect();

    let pid = nix::unistd::Pid::from_raw(server.pid() as i32);
    nix::sys::signal::kill(pid, nix::sys::signal::SIGUSR1).expect("kill(SIGUSR1)");

    // Give the signal a moment to be delivered and processed.
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        c.send(b"put 0 0 60 1\r\nx\r\n");
        let reply = c.read_line();
        if reply == "DRAINING\r\n" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "server never entered drain mode; last reply: {reply:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

// ---------------------------------------------------------------------
// concurrency / resource cleanup
// ---------------------------------------------------------------------

#[test]
fn one_thousand_connections_open_and_close_cleanly() {
    // Best-effort: raise our own soft RLIMIT_NOFILE, since we are about to
    // hold ~1,000 sockets open at once ourselves.
    if let Ok((soft, hard)) =
        nix::sys::resource::getrlimit(nix::sys::resource::Resource::RLIMIT_NOFILE)
        && hard > soft
    {
        let _ =
            nix::sys::resource::setrlimit(nix::sys::resource::Resource::RLIMIT_NOFILE, hard, hard);
    }

    let server = Server::start(&[]);

    const N: usize = 1000;
    let mut conns = Vec::with_capacity(N);
    for _ in 0..N {
        let mut c = server.connect();
        c.send(b"stats\r\n");
        let _ = c.read_body_reply();
        conns.push(c);
    }
    assert_responsive(&server);
    drop(conns);

    // Poll briefly: closing 1,000 sockets and having the server notice EOF
    // on each is not instantaneous.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut c = server.connect();
        c.send(b"stats\r\n");
        let (_, body) = c.read_body_reply();
        let body = String::from_utf8_lossy(&body);
        if body.contains("current-connections: 1") {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "current-connections never returned to 1; last body snippet: {}",
            body.lines()
                .find(|l| l.starts_with("current-connections"))
                .unwrap_or("<missing>")
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// A fresh connection can still complete a full round trip; used to prove
/// the server isn't wedged under load.
fn assert_responsive(server: &Server) {
    let mut c = server.connect();
    c.send(b"stats\r\n");
    let (header, _) = c.read_body_reply();
    assert!(
        header.starts_with("OK "),
        "server not responsive: {header:?}"
    );
}

/// A client blocked in reserve keeps pipelining far more than the server
/// buffers while waiting. The excess must be held back by TCP flow control,
/// not dropped: once the reserve resolves, every pipelined command is still
/// answered, in order.
#[test]
fn large_pipeline_behind_blocked_reserve_is_processed_in_order() {
    let server = Server::start(&[]);
    let mut reserver = server.connect();
    let mut putter = server.connect();

    reserver.send(b"reserve\r\n");
    assert!(
        !reserver.recv_something(Duration::from_millis(200)),
        "reserve resolved before any job existed"
    );

    // ~200 KiB of pipelined commands, well past the 64 KiB read cap. Written
    // from a separate thread since the server may stop reading (TCP flow
    // control) until the reserve resolves.
    const N: usize = 10_000;
    let pipeline = b"list-tube-used\r\n".repeat(N);
    let mut writer = reserver.stream.try_clone().expect("clone stream");
    let sender = std::thread::spawn(move || {
        use std::io::Write;
        writer.write_all(&pipeline).expect("write pipeline");
    });

    std::thread::sleep(Duration::from_millis(200));
    putter.send(b"put 0 0 60 2\r\nhi\r\n");
    assert_eq!(putter.read_line(), "INSERTED 1\r\n");

    let (header, body) = reserver.read_body_reply();
    assert_eq!(header, "RESERVED 1 2\r\n");
    assert_eq!(body, b"hi");
    for _ in 0..N {
        assert_eq!(reserver.read_line(), "USING default\r\n");
    }
    sender.join().expect("sender thread");
}
