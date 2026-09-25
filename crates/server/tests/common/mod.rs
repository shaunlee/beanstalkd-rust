//! Shared helpers for the black-box tests: spawn the real `beanstalkd-rs`
//! binary on a free port and drive it over raw TCP.

#![allow(clippy::unwrap_used)]
// Each test binary uses a different subset of these helpers.
#![allow(dead_code)]

use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(3);
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
pub struct Server {
    pub child: Child,
    pub addr: SocketAddr,
}

impl Server {
    pub fn start(extra_args: &[&str]) -> Server {
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

    pub fn connect(&self) -> Client {
        let s = TcpStream::connect_timeout(&self.addr, Duration::from_secs(2)).expect("connect");
        s.set_read_timeout(Some(DEFAULT_TIMEOUT))
            .expect("set_read_timeout");
        Client {
            stream: s,
            buf: Vec::new(),
        }
    }

    pub fn pid(&self) -> u32 {
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
pub struct Client {
    pub stream: TcpStream,
    buf: Vec<u8>,
}

impl Client {
    pub fn send(&mut self, data: &[u8]) {
        self.stream.write_all(data).expect("write");
    }

    pub fn shutdown_write(&mut self) {
        self.stream
            .shutdown(std::net::Shutdown::Write)
            .expect("shutdown(write)");
    }

    /// Reads and consumes one `\r\n`-terminated line (CRLF included),
    /// refilling from the socket as needed. Panics on EOF or a read error.
    pub fn read_line(&mut self) -> String {
        loop {
            if let Some(pos) = find_crlf(&self.buf) {
                let line: Vec<u8> = self.buf.drain(..pos + 2).collect();
                return String::from_utf8_lossy(&line).into_owned();
            }
            self.fill_more();
        }
    }

    /// Reads and consumes exactly `n` bytes.
    pub fn read_n(&mut self, n: usize) -> Vec<u8> {
        while self.buf.len() < n {
            self.fill_more();
        }
        self.buf.drain(..n).collect()
    }

    /// Reads a header line of the form `<prefix><n>\r\n` (e.g. `OK 1043`,
    /// `RESERVED 1 5`) followed by exactly `n + 2` more bytes (the body and
    /// its trailing CRLF), and returns `(header_line, body)` with `body`
    /// stripped of the trailing CRLF.
    pub fn read_body_reply(&mut self) -> (String, Vec<u8>) {
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
    pub fn recv_something(&mut self, timeout: Duration) -> bool {
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

pub fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}

impl Server {
    /// Sends `sig` (SIGTERM, SIGINT or SIGKILL) and waits up to 10 s for
    /// the process to exit. The port stays claimed until drop.
    pub fn stop(&mut self, sig: nix::sys::signal::Signal) -> std::process::ExitStatus {
        let pid = nix::unistd::Pid::from_raw(i32::try_from(self.child.id()).unwrap());
        nix::sys::signal::kill(pid, sig).expect("kill");
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = self.child.try_wait().expect("try_wait") {
                return status;
            }
            assert!(Instant::now() < deadline, "server did not exit after {sig}");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// Everything the server wrote to stderr (after it has exited).
    pub fn stderr(&mut self) -> String {
        let mut err = String::new();
        if let Some(mut e) = self.child.stderr.take() {
            let _ = e.read_to_string(&mut err);
        }
        err
    }
}

/// Runs the binary with `args` (plus `-l 127.0.0.1 -p <free port>`) and
/// waits for it to exit on its own; returns its status and stderr.
pub fn run_to_exit(args: &[&str]) -> (std::process::ExitStatus, String) {
    let port = claim_free_port();
    let mut child = Command::new(env!("CARGO_BIN_EXE_beanstalkd-rs"))
        .arg("-l")
        .arg("127.0.0.1")
        .arg("-p")
        .arg(port.to_string())
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn beanstalkd-rs");
    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            release_port(port);
            panic!("beanstalkd-rs {args:?} did not exit on its own");
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    release_port(port);
    let mut err = String::new();
    if let Some(mut e) = child.stderr.take() {
        let _ = e.read_to_string(&mut err);
    }
    (status, err)
}

impl Client {
    /// Sends `line` (CRLF appended) and returns the one-line reply without
    /// its CRLF.
    pub fn cmd(&mut self, line: &str) -> String {
        self.send(format!("{line}\r\n").as_bytes());
        self.read_line().trim_end_matches("\r\n").to_owned()
    }

    /// Sends a put and returns the reply line.
    pub fn put(&mut self, pri: u32, delay: u32, ttr: u32, body: &[u8]) -> String {
        let mut msg = format!("put {pri} {delay} {ttr} {}\r\n", body.len()).into_bytes();
        msg.extend_from_slice(body);
        msg.extend_from_slice(b"\r\n");
        self.send(&msg);
        self.read_line().trim_end_matches("\r\n").to_owned()
    }

    /// Sends a command with a YAML (`OK <n>`) reply and returns the body.
    pub fn yaml(&mut self, line: &str) -> String {
        self.send(format!("{line}\r\n").as_bytes());
        let (header, body) = self.read_body_reply();
        assert!(header.starts_with("OK "), "{line}: {header:?}");
        String::from_utf8(body).unwrap()
    }

    /// The value of `key` in a YAML reply to `line`.
    pub fn stat(&mut self, line: &str, key: &str) -> String {
        let yaml = self.yaml(line);
        yaml.lines()
            .find_map(|l| l.strip_prefix(&format!("{key}: ")))
            .unwrap_or_else(|| panic!("{key} missing from {yaml}"))
            .trim()
            .to_owned()
    }
}
