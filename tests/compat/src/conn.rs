//! Connection handling and step execution: turns a parsed [`crate::dsl::Step`]
//! sequence into a recorded sequence of [`Outcome`]s against one live server.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use crate::dsl::{Signal, Step, StepKind, StopMode};
use crate::mask::{MaskMode, mask_response_with};
use crate::server::ServerProcess;

/// The observable result of executing one DSL step against one server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// `send`: number of bytes written.
    Sent(usize),
    /// `recv`: the full raw response (status line + any body), unmasked.
    Received(Vec<u8>),
    /// `recv`: no complete response arrived within the timeout.
    Timeout,
    /// `recv_none`: nothing arrived within the window.
    NoBytes,
    /// `recv_none`: some bytes did arrive within the window (captured raw).
    SomeBytes(Vec<u8>),
    /// `recv_closed`: the peer closed the connection within the window.
    Closed,
    /// `recv_closed`: the connection was still open at the end of the window.
    StillOpen,
    /// `sleep` completed.
    Slept,
    /// `signal` was delivered to the server process.
    Signaled,
    /// `shutdown_write` completed.
    ShutdownDone,
    /// `close` completed.
    ClosedDone,
    /// `restart` / `crash` completed: the server was stopped and is
    /// accepting connections again; all connections were dropped.
    Restarted(StopMode),
    /// The harness itself failed to perform a step (e.g. the server could
    /// not be restarted). Never equal to anything, so it always surfaces
    /// as a mismatch.
    HarnessError(String),
    /// An unexpected I/O error occurred while performing the step (e.g. the
    /// connection could not be opened, or a write failed). Compared only by
    /// category, not by the OS-specific message text, to avoid flakiness.
    IoError(String),
}

const RECV_TIMEOUT: Duration = Duration::from_secs(3);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

struct ConnHandle {
    stream: TcpStream,
    buf: Vec<u8>,
    closed_seen: bool,
}

impl ConnHandle {
    fn connect(addr: SocketAddr) -> std::io::Result<Self> {
        let stream = TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT)?;
        stream.set_nodelay(true).ok();
        Ok(ConnHandle {
            stream,
            buf: Vec::new(),
            closed_seen: false,
        })
    }

    fn send(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.stream.write_all(data)?;
        Ok(data.len())
    }

    /// Try to read more bytes into `self.buf`, blocking at most until
    /// `deadline`. Returns the number of bytes read (0 on timeout or EOF;
    /// check `self.closed_seen` to tell them apart).
    fn fill(&mut self, deadline: Instant) -> std::io::Result<usize> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(0);
        }
        // On macOS, setsockopt(SO_RCVTIMEO) fails with EINVAL once the
        // connection is fully shut down (we half-closed and the peer
        // closed), although unread data may still be buffered. Fall back
        // to a non-blocking read then: it returns buffered data or EOF.
        let nonblocking = self.stream.set_read_timeout(Some(remaining)).is_err();
        if nonblocking {
            self.stream.set_nonblocking(true)?;
        }
        let mut tmp = [0u8; 4096];
        let res = self.stream.read(&mut tmp);
        if nonblocking {
            let _ = self.stream.set_nonblocking(false);
            if matches!(&res, Err(e) if e.kind() == std::io::ErrorKind::WouldBlock) {
                // Not expected on a fully shut down socket; avoid spinning.
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        match res {
            Ok(0) => {
                self.closed_seen = true;
                Ok(0)
            }
            Ok(n) => {
                self.buf.extend_from_slice(&tmp[..n]);
                Ok(n)
            }
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                Ok(0)
            }
            Err(e) => Err(e),
        }
    }

    fn recv_response(&mut self) -> Outcome {
        let deadline = Instant::now() + RECV_TIMEOUT;
        loop {
            if let Some(pos) = find(&self.buf, b"\r\n") {
                let extra = extra_body_len(&self.buf[..pos]);
                let total_needed = pos + 2 + extra.unwrap_or(0);
                if self.buf.len() >= total_needed {
                    let resp: Vec<u8> = self.buf.drain(..total_needed).collect();
                    return Outcome::Received(resp);
                }
            }
            if self.closed_seen {
                return Outcome::Timeout;
            }
            if Instant::now() >= deadline {
                return Outcome::Timeout;
            }
            match self.fill(deadline) {
                Ok(_) => {}
                Err(_) => return Outcome::Timeout,
            }
        }
    }

    fn recv_none(&mut self, dur: Duration) -> Outcome {
        if !self.buf.is_empty() {
            return Outcome::SomeBytes(self.buf.clone());
        }
        let deadline = Instant::now() + dur;
        match self.fill(deadline) {
            Ok(0) => Outcome::NoBytes,
            Ok(_) => Outcome::SomeBytes(self.buf.clone()),
            Err(_) => Outcome::NoBytes,
        }
    }

    fn recv_closed(&mut self, dur: Duration) -> Outcome {
        if self.closed_seen {
            return Outcome::Closed;
        }
        let deadline = Instant::now() + dur;
        loop {
            match self.fill(deadline) {
                Ok(0) => {
                    return if self.closed_seen {
                        Outcome::Closed
                    } else {
                        Outcome::StillOpen
                    };
                }
                Ok(_) => {
                    if Instant::now() >= deadline {
                        return Outcome::StillOpen;
                    }
                }
                Err(_) => return Outcome::StillOpen,
            }
        }
    }

    fn shutdown_write(&mut self) -> Outcome {
        match self.stream.shutdown(Shutdown::Write) {
            Ok(()) => Outcome::ShutdownDone,
            Err(e) => Outcome::IoError(e.to_string()),
        }
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if haystack.len() < needle.len() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// If `line` is a status line that is followed by a data chunk
/// (`RESERVED <id> <n>`, `FOUND <id> <n>`, `OK <n>`), return `n + 2` (the
/// body plus its trailing CRLF). Otherwise, `None`.
fn extra_body_len(line: &[u8]) -> Option<usize> {
    let s = std::str::from_utf8(line).ok()?;
    let mut it = s.split(' ').filter(|tok| !tok.is_empty());
    let head = it.next()?;
    let n: usize = match head {
        "RESERVED" | "FOUND" => {
            it.next()?;
            it.next()?.parse().ok()?
        }
        "OK" => it.next()?.parse().ok()?,
        _ => return None,
    };
    Some(n + 2)
}

/// Execute a whole case's steps against one server, returning one
/// [`Outcome`] per step (same length and order as `steps`). `restart` /
/// `crash` steps restart `server` in place.
pub fn execute(steps: &[Step], server: &mut ServerProcess) -> Vec<Outcome> {
    let mut conns: HashMap<String, ConnHandle> = HashMap::new();
    let mut out = Vec::with_capacity(steps.len());

    for step in steps {
        let outcome = match &step.kind {
            StepKind::Restart { how, downtime } => {
                // Every connection is invalidated; later references to a
                // connection name open a fresh one.
                conns.clear();
                match server.restart(*how, *downtime) {
                    Ok(()) => Outcome::Restarted(*how),
                    Err(e) => Outcome::HarnessError(format!("{} failed: {e}", how.directive())),
                }
            }
            kind => run_step(&mut conns, server.addr(), server.pid(), kind),
        };
        out.push(outcome);
    }
    out
}

fn get_or_open<'a>(
    conns: &'a mut HashMap<String, ConnHandle>,
    name: &str,
    addr: SocketAddr,
) -> Result<&'a mut ConnHandle, std::io::Error> {
    if !conns.contains_key(name) {
        let handle = ConnHandle::connect(addr)?;
        conns.insert(name.to_string(), handle);
    }
    Ok(conns
        .get_mut(name)
        .expect("connection was just inserted above"))
}

/// Deliver `sig` to process `pid` via `kill(1)` (keeps this crate free of
/// `unsafe` and of a libc dependency). Delivery is asynchronous on the
/// server side, so cases should `sleep` briefly afterwards.
fn send_signal(pid: u32, sig: Signal) -> Outcome {
    match std::process::Command::new("kill")
        .arg(format!("-{}", sig.name()))
        .arg(pid.to_string())
        .status()
    {
        Ok(status) if status.success() => Outcome::Signaled,
        Ok(status) => Outcome::IoError(format!("kill exited with {status}")),
        Err(e) => Outcome::IoError(format!("failed to run kill: {e}")),
    }
}

fn run_step(
    conns: &mut HashMap<String, ConnHandle>,
    addr: SocketAddr,
    pid: u32,
    kind: &StepKind,
) -> Outcome {
    match kind {
        StepKind::Send { conn, data } => match get_or_open(conns, conn, addr) {
            Ok(c) => match c.send(data) {
                Ok(n) => Outcome::Sent(n),
                Err(e) => Outcome::IoError(e.to_string()),
            },
            Err(e) => Outcome::IoError(e.to_string()),
        },
        StepKind::Recv { conn } => match get_or_open(conns, conn, addr) {
            Ok(c) => c.recv_response(),
            Err(e) => Outcome::IoError(e.to_string()),
        },
        StepKind::RecvNone { conn, dur } => match get_or_open(conns, conn, addr) {
            Ok(c) => c.recv_none(*dur),
            Err(e) => Outcome::IoError(e.to_string()),
        },
        StepKind::RecvClosed { conn, dur } => match get_or_open(conns, conn, addr) {
            Ok(c) => c.recv_closed(*dur),
            Err(e) => Outcome::IoError(e.to_string()),
        },
        StepKind::Sleep(dur) => {
            std::thread::sleep(*dur);
            Outcome::Slept
        }
        StepKind::Signal(sig) => send_signal(pid, *sig),
        StepKind::ShutdownWrite { conn } => match get_or_open(conns, conn, addr) {
            Ok(c) => c.shutdown_write(),
            Err(e) => Outcome::IoError(e.to_string()),
        },
        StepKind::Close { conn } => {
            conns.remove(conn);
            Outcome::ClosedDone
        }
        StepKind::Restart { how, .. } => Outcome::HarnessError(format!(
            "internal error: {} must be handled by execute()",
            how.directive()
        )),
    }
}

/// Equality for comparing two outcomes across two independent server
/// processes: bodies of `Received`/`SomeBytes` responses are masked first,
/// and `IoError` is compared only by category (not by exact message) since
/// OS-level error text can vary in irrelevant ways between processes.
/// `HarnessError` never compares equal. Uses the default mask mode; see
/// [`outcomes_equal_with`].
pub fn outcomes_equal(a: &Outcome, b: &Outcome) -> bool {
    outcomes_equal_with(a, b, MaskMode::default())
}

/// Like [`outcomes_equal`], masking responses according to `mode`.
pub fn outcomes_equal_with(a: &Outcome, b: &Outcome, mode: MaskMode) -> bool {
    match (a, b) {
        (Outcome::Received(x), Outcome::Received(y)) => {
            mask_response_with(x, mode) == mask_response_with(y, mode)
        }
        (Outcome::IoError(_), Outcome::IoError(_)) => true,
        (Outcome::HarnessError(_), _) | (_, Outcome::HarnessError(_)) => false,
        _ => a == b,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Data buffered on a socket that we half-closed and the peer then
    /// closed must still be readable (macOS rejects SO_RCVTIMEO there).
    #[test]
    fn buffered_data_is_read_after_both_sides_shut_down() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let mut conn = ConnHandle::connect(addr).expect("connect");
        let (mut server, _) = listener.accept().expect("accept");
        conn.stream.shutdown(Shutdown::Write).expect("shutdown");
        server.write_all(b"ONE\r\n").expect("write");
        server.write_all(b"TWO\r\n").expect("write");
        drop(server);
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(conn.recv_response(), Outcome::Received(b"ONE\r\n".to_vec()));
        assert_eq!(conn.recv_response(), Outcome::Received(b"TWO\r\n".to_vec()));
        assert_eq!(
            conn.recv_closed(Duration::from_millis(200)),
            Outcome::Closed
        );
    }

    #[test]
    fn extra_body_len_parses_reserved_found_ok() {
        assert_eq!(extra_body_len(b"RESERVED 1 5"), Some(7));
        assert_eq!(extra_body_len(b"FOUND 42 3"), Some(5));
        assert_eq!(extra_body_len(b"OK 100"), Some(102));
        assert_eq!(extra_body_len(b"DELETED"), None);
        assert_eq!(extra_body_len(b"NOT_FOUND"), None);
    }

    #[test]
    fn outcomes_equal_masks_received_bodies() {
        let a = Outcome::Received(b"OK 9\r\npid: 111\n\r\n".to_vec());
        let b = Outcome::Received(b"OK 9\r\npid: 222\n\r\n".to_vec());
        assert!(outcomes_equal(&a, &b));
    }

    #[test]
    fn outcomes_equal_ignores_io_error_message_text() {
        let a = Outcome::IoError("connection refused".to_string());
        let b = Outcome::IoError("broken pipe".to_string());
        assert!(outcomes_equal(&a, &b));
    }

    #[test]
    fn harness_errors_never_compare_equal() {
        let e = Outcome::HarnessError("restart failed".to_string());
        assert!(!outcomes_equal(&e, &e.clone()));
        assert!(!outcomes_equal(&e, &Outcome::Restarted(StopMode::Term)));
        assert!(outcomes_equal(
            &Outcome::Restarted(StopMode::Kill),
            &Outcome::Restarted(StopMode::Kill)
        ));
    }

    #[test]
    fn binlog_mode_masks_file_field() {
        let a = Outcome::Received(b"OK 18\r\n---\nid: 1\nfile: 2\n\r\n".to_vec());
        let b = Outcome::Received(b"OK 18\r\n---\nid: 1\nfile: 5\n\r\n".to_vec());
        assert!(!outcomes_equal(&a, &b));
        assert!(outcomes_equal_with(&a, &b, MaskMode { binlog: true }));
    }

    #[test]
    fn outcomes_not_equal_for_different_variants() {
        assert!(!outcomes_equal(&Outcome::Timeout, &Outcome::NoBytes));
    }
}
