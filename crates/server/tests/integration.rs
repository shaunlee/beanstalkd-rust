//! Black-box integration tests for the `beanstalkd-rs` binary: spawn the
//! real binary and drive it over raw TCP, exactly as a client would. Per
//! T4 scope, these only touch the `bstk-server` crate's public artifact
//! (the compiled binary); they know nothing about its internals.

#![allow(clippy::unwrap_used)]

use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(3);
const SPAWN_ATTEMPTS: u32 = 5;

/// Ports currently claimed by a `Server` somewhere in this test binary.
///
/// `TcpListener::bind(("127.0.0.1", 0))` picks a free ephemeral port, but
/// there is a gap between releasing that listener and the child process
/// actually binding the same port. With `cargo test`'s default parallel
/// test threads, two tests can race and get handed the *same* ephemeral
/// port in that gap: whichever child loses the `bind()` race exits with
/// `AddrInUse`, while the winning test's `wait_until_accepting` can end up
/// connecting to a *different* test's already-running server on that same
/// port -- which then gets killed out from under it when that other test
/// finishes, producing a spurious `ConnectionReset`. Holding the port for
/// the entire lifetime of the `Server`, not just during spawn, closes that
/// race within this process (mirrors `tests/compat/src/server.rs`).
static CLAIMED_PORTS: LazyLock<Mutex<HashSet<u16>>> = LazyLock::new(|| Mutex::new(HashSet::new()));

fn claim_free_port() -> u16 {
    loop {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind free port");
        let port = listener.local_addr().expect("local_addr").port();
        drop(listener);
        let mut claimed = CLAIMED_PORTS.lock().expect("claimed-ports lock poisoned");
        if claimed.insert(port) {
            return port;
        }
    }
}

fn release_port(port: u16) {
    CLAIMED_PORTS
        .lock()
        .expect("claimed-ports lock poisoned")
        .remove(&port);
}

/// A running server process on a free `127.0.0.1` port, killed on drop so a
/// failing test never leaves an orphan process behind.
struct Server {
    child: Child,
    addr: SocketAddr,
}

impl Server {
    fn start(extra_args: &[&str]) -> Server {
        let mut last_err = None;
        for _ in 0..SPAWN_ATTEMPTS {
            let port = claim_free_port();
            match Self::try_start(port, extra_args) {
                Ok(server) => return server,
                Err(e) => {
                    release_port(port);
                    last_err = Some(e);
                }
            }
        }
        panic!("server failed to start after {SPAWN_ATTEMPTS} attempts: {last_err:?}");
    }

    fn try_start(port: u16, extra_args: &[&str]) -> Result<Server, String> {
        let bin = env!("CARGO_BIN_EXE_beanstalkd-rs");
        let mut cmd = Command::new(bin);
        cmd.arg("-l")
            .arg("127.0.0.1")
            .arg("-p")
            .arg(port.to_string())
            .args(extra_args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let child = cmd.spawn().expect("spawn beanstalkd-rs");
        let addr: SocketAddr = ([127, 0, 0, 1], port).into();

        let mut server = Server { child, addr };
        match server.wait_until_accepting() {
            Ok(()) => Ok(server),
            Err(e) => {
                let _ = server.child.kill();
                let _ = server.child.wait();
                Err(e)
            }
        }
    }

    /// Waits until a real request/response round trip succeeds, not a bare
    /// connect: under a port-claim race (see `CLAIMED_PORTS`), the *other*
    /// side's leftover listener can accept a TCP connection without this
    /// process ever having bound the port, so a full `quit` exchange is
    /// used to make sure it is truly *our* child answering.
    fn wait_until_accepting(&mut self) -> Result<(), String> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Ok(Some(status)) = self.child.try_wait() {
                let mut err = String::new();
                if let Some(mut e) = self.child.stderr.take() {
                    let _ = e.read_to_string(&mut err);
                }
                return Err(format!("server exited early with {status}; stderr={err:?}"));
            }
            if self.probe_ready() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err("server did not start accepting connections in time".to_owned());
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn probe_ready(&self) -> bool {
        let Ok(mut s) = TcpStream::connect_timeout(&self.addr, Duration::from_millis(150)) else {
            return false;
        };
        if s.set_read_timeout(Some(Duration::from_millis(300)))
            .is_err()
        {
            return false;
        }
        if s.write_all(b"quit\r\n").is_err() {
            return false;
        }
        let mut buf = [0u8; 16];
        // `quit` gives no reply and closes; a clean read (even `Ok(0)`) is
        // enough confirmation that our own child processed the byte.
        s.read(&mut buf).is_ok()
    }

    fn connect(&self) -> Client {
        let s = TcpStream::connect_timeout(&self.addr, Duration::from_secs(2)).expect("connect");
        s.set_read_timeout(Some(DEFAULT_TIMEOUT))
            .expect("set_read_timeout");
        Client {
            stream: s,
            buf: Vec::new(),
        }
    }

    fn pid(&self) -> u32 {
        self.child.id()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let panicking = std::thread::panicking();
        let _ = self.child.kill();
        let _ = self.child.wait();
        if panicking {
            let mut out = String::new();
            let mut err = String::new();
            if let Some(mut o) = self.child.stdout.take() {
                let _ = o.read_to_string(&mut out);
            }
            if let Some(mut e) = self.child.stderr.take() {
                let _ = e.read_to_string(&mut err);
            }
            eprintln!("--- server stdout ---\n{out}\n--- server stderr ---\n{err}");
        }
        release_port(self.addr.port());
    }
}

/// A connection with its own leftover-bytes buffer, so replies can be
/// consumed one line (or one exact byte count) at a time even when the
/// server's single `write()` delivered several replies -- or a reply plus
/// the start of the next one -- in one `read()`.
struct Client {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Client {
    fn send(&mut self, data: &[u8]) {
        self.stream.write_all(data).expect("write");
    }

    fn shutdown_write(&mut self) {
        self.stream
            .shutdown(std::net::Shutdown::Write)
            .expect("shutdown(write)");
    }

    /// Reads and consumes one `\r\n`-terminated line (CRLF included),
    /// refilling from the socket as needed. Panics on EOF or a read error.
    fn read_line(&mut self) -> String {
        loop {
            if let Some(pos) = find_crlf(&self.buf) {
                let line: Vec<u8> = self.buf.drain(..pos + 2).collect();
                return String::from_utf8_lossy(&line).into_owned();
            }
            self.fill_more();
        }
    }

    /// Reads and consumes exactly `n` bytes.
    fn read_n(&mut self, n: usize) -> Vec<u8> {
        while self.buf.len() < n {
            self.fill_more();
        }
        self.buf.drain(..n).collect()
    }

    /// Reads a header line of the form `<prefix><n>\r\n` (e.g. `OK 1043`,
    /// `RESERVED 1 5`) followed by exactly `n + 2` more bytes (the body and
    /// its trailing CRLF), and returns `(header_line, body)` with `body`
    /// stripped of the trailing CRLF.
    fn read_body_reply(&mut self) -> (String, Vec<u8>) {
        let header = self.read_line();
        let n: usize = header
            .trim_end_matches("\r\n")
            .rsplit(' ')
            .next()
            .expect("a trailing byte-count field")
            .parse()
            .unwrap_or_else(|e| panic!("byte count in {header:?}: {e}"));
        let mut body = self.read_n(n + 2);
        body.truncate(n);
        (header, body)
    }

    /// Blocks up to `timeout` for more bytes to arrive; returns `true` if
    /// any did (buffered for the next read), `false` on a plain timeout.
    /// Used to assert "no reply yet" for a genuinely blocking command.
    fn recv_something(&mut self, timeout: Duration) -> bool {
        if !self.buf.is_empty() {
            return true;
        }
        self.stream
            .set_read_timeout(Some(timeout))
            .expect("set_read_timeout");
        let mut chunk = [0u8; 4096];
        let got = match self.stream.read(&mut chunk) {
            Ok(0) => true, // EOF is also "something happened"
            Ok(n) => {
                self.buf.extend_from_slice(&chunk[..n]);
                true
            }
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                false
            }
            Err(e) => panic!("unexpected read error: {e}"),
        };
        self.stream
            .set_read_timeout(Some(DEFAULT_TIMEOUT))
            .expect("reset read timeout");
        got
    }

    fn fill_more(&mut self) {
        let mut chunk = [0u8; 4096];
        let n = self.stream.read(&mut chunk).expect("read");
        assert!(n > 0, "connection closed unexpectedly");
        self.buf.extend_from_slice(&chunk[..n]);
    }
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}

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
