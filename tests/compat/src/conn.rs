//! Connection handling and step execution: turns a parsed [`crate::dsl::Step`]
//! sequence into a recorded sequence of [`Outcome`]s against one live server.
//!
//! # Transports
//!
//! A connection is either plaintext TCP or TLS over TCP ([`ClientTransport`],
//! chosen per server). Every DSL action has the same observable semantics
//! over both:
//!
//! | action | plaintext | TLS |
//! |---|---|---|
//! | open (lazy) | TCP connect | TCP connect, then the full TLS handshake, eagerly, before the step runs; with TLS 1.3, also a wait of up to 250 ms for the server's session tickets (consumed, never visible to the case) |
//! | `send` | write all bytes | write all bytes as application data and flush every TLS record to the socket |
//! | `recv` / `recv_none` | read TCP bytes into the buffer | read TLS records, append the decrypted application data to the same buffer (framing is done on plaintext, identically) |
//! | `recv_closed` = [`Outcome::Closed`] | TCP EOF (`read` returns 0) | a TLS `close_notify` alert **or** a TCP EOF without one (truncation is reported as closed, not as an error) |
//! | `shutdown_write` | `shutdown(SHUT_WR)` (TCP FIN) | queue a `close_notify` alert, flush it, **then** `shutdown(SHUT_WR)`: the server first sees a clean TLS end of stream, then the TCP FIN; reading continues to work afterwards (TLS 1.3 and rustls allow receiving after sending `close_notify`) |
//! | `send` after `shutdown_write` | `EPIPE` ([`Outcome::IoError`]) | `BrokenPipe` ([`Outcome::IoError`]), without touching the socket |
//! | `close` | close the socket | consume already-arrived TLS records, send a `close_notify` (best effort, unless already sent by `shutdown_write`), then close the socket |
//! | `restart` / `crash` | drop every connection | drop every connection without `close_notify` (the server process is gone) |
//!
//! Errors other than an end of stream (e.g. a connection reset) are treated
//! the same way on both transports.
//!
//! Session tickets are the one kind of data a TLS server sends without
//! being asked. Left unread, they would turn a later `close` into a TCP RST
//! (the kernel resets connections closed with unread data), which can make
//! the server lose requests it has not read yet; plaintext has no such
//! data. Hence the ticket wait on open and the drain on `close`. Unread
//! *application* data is drained by `close` too, so a `close` with unread
//! responses is a FIN over TLS but a RST over plaintext; cases should read
//! what they provoke before closing (a race over plaintext anyway).

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rustls::{ClientConfig, ClientConnection, StreamOwned};

use crate::dsl::{Signal, Step, StepKind, StopMode};
use crate::mask::{MaskMode, mask_response_with};
use crate::server::ServerProcess;
use crate::tls::TlsMaterial;

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
/// How long a new TLS 1.3 connection waits for the server's session tickets
/// (see `tls_handshake`).
const TICKET_WAIT: Duration = Duration::from_millis(250);

/// How the harness talks to one server.
#[derive(Clone, Default)]
pub enum ClientTransport {
    /// Plain TCP.
    #[default]
    Plain,
    /// TLS over TCP, trusting only the configured roots; the server
    /// certificate is verified against [`crate::tls::TLS_SERVER_NAME`].
    Tls(Arc<ClientConfig>),
}

impl std::fmt::Debug for ClientTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientTransport::Plain => f.write_str("Plain"),
            ClientTransport::Tls(_) => f.write_str("Tls"),
        }
    }
}

impl ClientTransport {
    /// Whether this transport uses TLS.
    pub fn is_tls(&self) -> bool {
        matches!(self, ClientTransport::Tls(_))
    }
}

type TlsStream = StreamOwned<ClientConnection, TcpStream>;

enum Stream {
    Plain(TcpStream),
    // Boxed: a `ClientConnection` is much larger than a `TcpStream`.
    Tls(Box<TlsStream>),
}

impl Stream {
    #[cfg(test)]
    fn tcp(&self) -> &TcpStream {
        match self {
            Stream::Plain(s) => s,
            Stream::Tls(s) => &s.sock,
        }
    }
}

/// A client connection over either transport (see the module docs).
pub struct ConnHandle {
    stream: Stream,
    buf: Vec<u8>,
    closed_seen: bool,
    /// `shutdown_write` was performed (TLS: `close_notify` already sent).
    write_closed: bool,
}

/// Open a connection to `addr` over `transport`. For TLS, the handshake is
/// completed before this returns (bounded by the connect timeout).
/// `connect_timeout` bounds the TCP connect only.
pub fn connect(
    addr: SocketAddr,
    transport: &ClientTransport,
    connect_timeout: Duration,
) -> std::io::Result<ConnHandle> {
    ConnHandle::connect_with(addr, transport, connect_timeout)
}

impl ConnHandle {
    fn connect(addr: SocketAddr, transport: &ClientTransport) -> std::io::Result<Self> {
        Self::connect_with(addr, transport, CONNECT_TIMEOUT)
    }

    fn connect_with(
        addr: SocketAddr,
        transport: &ClientTransport,
        connect_timeout: Duration,
    ) -> std::io::Result<Self> {
        let sock = TcpStream::connect_timeout(&addr, connect_timeout)?;
        sock.set_nodelay(true).ok();
        let stream = match transport {
            ClientTransport::Plain => Stream::Plain(sock),
            ClientTransport::Tls(config) => {
                Stream::Tls(Box::new(tls_handshake(sock, Arc::clone(config))?))
            }
        };
        Ok(ConnHandle {
            stream,
            buf: Vec::new(),
            closed_seen: false,
            write_closed: false,
        })
    }

    /// Write all of `data` (TLS: as application data, flushed to the
    /// socket).
    pub fn send(&mut self, data: &[u8]) -> std::io::Result<usize> {
        match &mut self.stream {
            Stream::Plain(s) => s.write_all(data)?,
            Stream::Tls(s) => {
                if self.write_closed {
                    // Data must not follow our close_notify; report the
                    // EPIPE a plaintext socket gives after SHUT_WR.
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "write after shutdown_write (close_notify sent)",
                    ));
                }
                let mut rest = data;
                while !rest.is_empty() {
                    let n = s.conn.writer().write(rest)?;
                    rest = &rest[n..];
                    // Also frees the plaintext buffer when it was full
                    // (`n == 0`).
                    flush_tls(s)?;
                }
            }
        }
        Ok(data.len())
    }

    /// Try to read more bytes into `self.buf`, blocking at most until
    /// `deadline`. Returns the number of bytes read (0 on timeout or EOF;
    /// check `self.closed_seen` to tell them apart).
    fn fill(&mut self, deadline: Instant) -> std::io::Result<usize> {
        match &mut self.stream {
            Stream::Plain(s) => fill_plain(s, &mut self.buf, &mut self.closed_seen, deadline),
            Stream::Tls(s) => fill_tls(s, &mut self.buf, &mut self.closed_seen, deadline),
        }
    }

    /// Read one complete response (status line plus any data chunk).
    pub fn recv_response(&mut self) -> Outcome {
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

    /// Record whether any bytes arrive within `dur`.
    pub fn recv_none(&mut self, dur: Duration) -> Outcome {
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

    /// Record whether the peer closes the connection within `dur`.
    pub fn recv_closed(&mut self, dur: Duration) -> Outcome {
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

    /// Half-close: plaintext `shutdown(SHUT_WR)`; TLS `close_notify`, then
    /// `shutdown(SHUT_WR)`.
    pub fn shutdown_write(&mut self) -> Outcome {
        let res = match &mut self.stream {
            Stream::Plain(s) => s.shutdown(Shutdown::Write),
            Stream::Tls(s) => {
                if !self.write_closed {
                    s.conn.send_close_notify();
                }
                flush_tls(s).and_then(|()| s.sock.shutdown(Shutdown::Write))
            }
        };
        self.write_closed = true;
        match res {
            Ok(()) => Outcome::ShutdownDone,
            Err(e) => Outcome::IoError(e.to_string()),
        }
    }

    /// Close the connection (TLS: best-effort `close_notify` first).
    pub fn close(mut self) {
        if let Stream::Tls(s) = &mut self.stream {
            // Consume TLS records that already arrived (e.g. session
            // tickets): closing a socket with unread data makes the kernel
            // send a RST instead of a FIN, which may discard data the
            // server has not read yet. Plaintext has no such records.
            drain_available(s);
            if !self.write_closed {
                s.conn.send_close_notify();
                // Never block on a peer that stopped reading.
                if s.sock.set_nonblocking(true).is_ok() {
                    let _ = flush_tls(s);
                }
            }
        }
        // Dropping the socket closes it.
    }
}

/// Run a TLS client handshake on `sock`, bounded by the connect timeout.
fn tls_handshake(sock: TcpStream, config: Arc<ClientConfig>) -> std::io::Result<TlsStream> {
    let conn =
        ClientConnection::new(config, TlsMaterial::server_name()).map_err(std::io::Error::other)?;
    let mut s = StreamOwned::new(conn, sock);
    let deadline = Instant::now() + CONNECT_TIMEOUT;
    while s.conn.is_handshaking() {
        flush_tls(&mut s)?;
        if !s.conn.is_handshaking() {
            break;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "TLS handshake timed out",
            ));
        }
        s.sock.set_read_timeout(Some(remaining))?;
        match s.conn.read_tls(&mut s.sock)? {
            0 => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed during the TLS handshake",
                ));
            }
            _ => process_tls(&mut s)?,
        }
    }
    // The client's Finished (and anything else pending).
    flush_tls(&mut s)?;

    // A TLS 1.3 server sends its session tickets right after it receives
    // our Finished. Wait (briefly) for them here, so that they are consumed
    // before the case's first step instead of lingering unread in the
    // socket: a `close` with unread data turns into a RST (see
    // `ConnHandle::close`), and waiting also means the server has finished
    // its side of the handshake when the first step runs. A server that
    // sends no tickets costs `TICKET_WAIT` per connection.
    if s.conn.protocol_version() == Some(rustls::ProtocolVersion::TLSv1_3) {
        s.sock.set_read_timeout(Some(TICKET_WAIT))?;
        match s.conn.read_tls(&mut s.sock) {
            // EOF: recorded by rustls, reported by the next read.
            Ok(0) => {}
            Ok(_) => process_tls(&mut s)?,
            Err(e) if is_timeout(&e) => {}
            Err(e) => return Err(e),
        }
        drain_available(&mut s);
    }
    s.sock.set_read_timeout(None)?;
    Ok(s)
}

/// Read and process every TLS record that is already available, without
/// blocking. Decrypted application data stays buffered in rustls (and is
/// returned by the next `fill_tls`). Errors are left for later reads to
/// report.
fn drain_available(s: &mut TlsStream) {
    if s.sock.set_nonblocking(true).is_err() {
        return;
    }
    while let Ok(n) = s.conn.read_tls(&mut s.sock) {
        if n == 0 || process_tls(s).is_err() {
            break;
        }
    }
    let _ = s.sock.set_nonblocking(false);
}

/// Write every pending TLS record to the socket.
fn flush_tls(s: &mut TlsStream) -> std::io::Result<()> {
    while s.conn.wants_write() {
        s.conn.write_tls(&mut s.sock)?;
    }
    Ok(())
}

/// Process freshly read TLS records; on a protocol error, try to send the
/// resulting alert and report the error.
fn process_tls(s: &mut TlsStream) -> std::io::Result<()> {
    if let Err(e) = s.conn.process_new_packets() {
        let _ = flush_tls(s);
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e));
    }
    Ok(())
}

/// Set a read timeout of `remaining` on `sock`. Returns `true` if that
/// failed and the socket was switched to non-blocking mode instead.
///
/// On macOS, setsockopt(SO_RCVTIMEO) fails with EINVAL once the connection
/// is fully shut down (we half-closed and the peer closed), although unread
/// data may still be buffered. A non-blocking read then returns buffered
/// data or EOF.
fn set_deadline(sock: &TcpStream, remaining: Duration) -> std::io::Result<bool> {
    let nonblocking = sock.set_read_timeout(Some(remaining)).is_err();
    if nonblocking {
        sock.set_nonblocking(true)?;
    }
    Ok(nonblocking)
}

/// Undo [`set_deadline`]'s non-blocking fallback after a read.
fn end_deadline<T>(sock: &TcpStream, nonblocking: bool, res: &std::io::Result<T>) {
    if nonblocking {
        let _ = sock.set_nonblocking(false);
        if matches!(res, Err(e) if e.kind() == std::io::ErrorKind::WouldBlock) {
            // Not expected on a fully shut down socket; avoid spinning.
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

fn is_timeout(e: &std::io::Error) -> bool {
    e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut
}

fn fill_plain(
    s: &mut TcpStream,
    buf: &mut Vec<u8>,
    closed_seen: &mut bool,
    deadline: Instant,
) -> std::io::Result<usize> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Ok(0);
    }
    let nonblocking = set_deadline(s, remaining)?;
    let mut tmp = [0u8; 4096];
    let res = s.read(&mut tmp);
    end_deadline(s, nonblocking, &res);
    match res {
        Ok(0) => {
            *closed_seen = true;
            Ok(0)
        }
        Ok(n) => {
            buf.extend_from_slice(&tmp[..n]);
            Ok(n)
        }
        Err(e) if is_timeout(&e) => Ok(0),
        Err(e) => Err(e),
    }
}

/// TLS counterpart of [`fill_plain`]: returns as soon as some application
/// data was decrypted, the stream ended, or `deadline` passed. Records that
/// carry no application data (e.g. TLS 1.3 session tickets) are consumed
/// without returning, so they never count as "bytes arrived".
fn fill_tls(
    s: &mut TlsStream,
    buf: &mut Vec<u8>,
    closed_seen: &mut bool,
    deadline: Instant,
) -> std::io::Result<usize> {
    let mut tmp = [0u8; 4096];
    loop {
        // Plaintext already decrypted takes priority over the socket.
        match s.conn.reader().read(&mut tmp) {
            // A close_notify was received (after all earlier data).
            Ok(0) => {
                *closed_seen = true;
                return Ok(0);
            }
            Ok(n) => {
                buf.extend_from_slice(&tmp[..n]);
                return Ok(n);
            }
            // No plaintext buffered and the stream is still open.
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            // TCP EOF without close_notify: the peer closed all the same.
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                *closed_seen = true;
                return Ok(0);
            }
            Err(e) => return Err(e),
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(0);
        }
        let nonblocking = set_deadline(&s.sock, remaining)?;
        let res = s.conn.read_tls(&mut s.sock);
        end_deadline(&s.sock, nonblocking, &res);
        match res {
            // `Ok(0)` is TCP EOF: rustls records it and the reader above
            // then reports either a clean close or a truncation.
            Ok(_) => {
                process_tls(s)?;
                // Answer anything the TLS layer needs to send (e.g. a key
                // update). Fails harmlessly once our write side is shut.
                let _ = flush_tls(s);
            }
            Err(e) if is_timeout(&e) => return Ok(0),
            Err(e) => return Err(e),
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
    let transport = server.client_transport();
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
            kind => run_step(&mut conns, server.addr(), &transport, server.pid(), kind),
        };
        out.push(outcome);
    }
    out
}

fn get_or_open<'a>(
    conns: &'a mut HashMap<String, ConnHandle>,
    name: &str,
    addr: SocketAddr,
    transport: &ClientTransport,
) -> Result<&'a mut ConnHandle, std::io::Error> {
    if !conns.contains_key(name) {
        let handle = ConnHandle::connect(addr, transport)?;
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
    transport: &ClientTransport,
    pid: u32,
    kind: &StepKind,
) -> Outcome {
    match kind {
        StepKind::Send { conn, data } => match get_or_open(conns, conn, addr, transport) {
            Ok(c) => match c.send(data) {
                Ok(n) => Outcome::Sent(n),
                Err(e) => Outcome::IoError(e.to_string()),
            },
            Err(e) => Outcome::IoError(e.to_string()),
        },
        StepKind::Recv { conn } => match get_or_open(conns, conn, addr, transport) {
            Ok(c) => c.recv_response(),
            Err(e) => Outcome::IoError(e.to_string()),
        },
        StepKind::RecvNone { conn, dur } => match get_or_open(conns, conn, addr, transport) {
            Ok(c) => c.recv_none(*dur),
            Err(e) => Outcome::IoError(e.to_string()),
        },
        StepKind::RecvClosed { conn, dur } => match get_or_open(conns, conn, addr, transport) {
            Ok(c) => c.recv_closed(*dur),
            Err(e) => Outcome::IoError(e.to_string()),
        },
        StepKind::Sleep(dur) => {
            std::thread::sleep(*dur);
            Outcome::Slept
        }
        StepKind::Signal(sig) => send_signal(pid, *sig),
        StepKind::ShutdownWrite { conn } => match get_or_open(conns, conn, addr, transport) {
            Ok(c) => c.shutdown_write(),
            Err(e) => Outcome::IoError(e.to_string()),
        },
        StepKind::Close { conn } => {
            if let Some(c) = conns.remove(conn) {
                c.close();
            }
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
        let mut conn = ConnHandle::connect(addr, &ClientTransport::Plain).expect("connect");
        let (mut server, _) = listener.accept().expect("accept");
        conn.stream
            .tcp()
            .shutdown(Shutdown::Write)
            .expect("shutdown");
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

    // ---- TLS transport, against a tiny in-test rustls server. ----

    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use rustls::{ServerConfig, ServerConnection};

    type ServerStream = StreamOwned<ServerConnection, TcpStream>;

    fn server_config(m: &TlsMaterial) -> Arc<ServerConfig> {
        let certs = CertificateDer::pem_file_iter(m.cert_path())
            .expect("open cert")
            .collect::<Result<Vec<_>, _>>()
            .expect("parse cert");
        let key = PrivateKeyDer::from_pem_file(m.key_path()).expect("parse key");
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let config = ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .expect("versions")
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .expect("server cert");
        Arc::new(config)
    }

    /// Start a one-connection TLS server running `serve` on the accepted,
    /// handshaken stream, and connect a TLS client to it.
    fn tls_pair<F>(serve: F) -> (ConnHandle, std::thread::JoinHandle<()>, TlsMaterial)
    where
        F: FnOnce(ServerStream) + Send + 'static,
    {
        let material = TlsMaterial::generate().expect("generate TLS material");
        let config = server_config(&material);
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let handle = std::thread::spawn(move || {
            let (sock, _) = listener.accept().expect("accept");
            sock.set_read_timeout(Some(Duration::from_secs(5)))
                .expect("timeout");
            let conn = ServerConnection::new(config).expect("server conn");
            let mut s = StreamOwned::new(conn, sock);
            while s.conn.is_handshaking() {
                s.conn.complete_io(&mut s.sock).expect("server handshake");
            }
            serve(s);
        });
        let transport = ClientTransport::Tls(material.client_config());
        let conn = ConnHandle::connect(addr, &transport).expect("TLS connect");
        (conn, handle, material)
    }

    /// Read application data until the end of the stream. Returns the data
    /// and whether the stream ended cleanly (close_notify) rather than by a
    /// bare TCP EOF.
    fn server_read_to_end(s: &mut ServerStream) -> (Vec<u8>, bool) {
        let mut data = Vec::new();
        let mut tmp = [0u8; 1024];
        loop {
            match s.read(&mut tmp) {
                Ok(0) => return (data, true),
                Ok(n) => data.extend_from_slice(&tmp[..n]),
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return (data, false),
                Err(e) => panic!("server read failed: {e}"),
            }
        }
    }

    /// Read exactly one CRLF-terminated line of application data.
    fn server_read_line(s: &mut ServerStream) -> Vec<u8> {
        let mut line = Vec::new();
        let mut byte = [0u8; 1];
        while !line.ends_with(b"\r\n") {
            s.read_exact(&mut byte).expect("server read");
            line.push(byte[0]);
        }
        line
    }

    fn server_close_notify(s: &mut ServerStream) {
        s.conn.send_close_notify();
        s.flush().expect("server flush");
        while s.conn.wants_write() {
            s.conn.write_tls(&mut s.sock).expect("server write_tls");
        }
    }

    #[test]
    fn tls_send_and_framed_recv() {
        let (mut conn, handle, _m) = tls_pair(|mut s| {
            assert_eq!(server_read_line(&mut s), b"put 0 0 1 5\r\n");
            assert_eq!(server_read_line(&mut s), b"hello\r\n");
            // A body response split over several TLS records.
            s.write_all(b"RESERVED 7 5\r\nhel").expect("write");
            s.flush().expect("flush");
            std::thread::sleep(Duration::from_millis(50));
            s.write_all(b"lo\r\nINSERTED 8\r\n").expect("write");
            s.flush().expect("flush");
            let _ = server_read_to_end(&mut s);
        });
        assert_eq!(conn.send(b"put 0 0 1 5\r\nhello\r\n").expect("send"), 20);
        assert_eq!(
            conn.recv_response(),
            Outcome::Received(b"RESERVED 7 5\r\nhello\r\n".to_vec())
        );
        assert_eq!(
            conn.recv_response(),
            Outcome::Received(b"INSERTED 8\r\n".to_vec())
        );
        conn.close();
        handle.join().expect("server thread");
    }

    /// A large send goes through even though it exceeds rustls's plaintext
    /// buffer limit.
    #[test]
    fn tls_large_send_is_fully_delivered() {
        let big = vec![b'x'; 300_000];
        let expected = big.len();
        let (mut conn, handle, _m) = tls_pair(move |mut s| {
            let (data, clean) = server_read_to_end(&mut s);
            assert_eq!(data.len(), expected);
            assert!(clean);
        });
        assert_eq!(conn.send(&big).expect("send"), expected);
        conn.close();
        handle.join().expect("server thread");
    }

    /// No application data arrives: TLS 1.3 session tickets sent after the
    /// handshake must not count as "bytes arrived".
    #[test]
    fn tls_recv_none_ignores_handshake_records() {
        let (mut conn, handle, _m) = tls_pair(|mut s| {
            let _ = server_read_to_end(&mut s);
        });
        assert_eq!(conn.recv_none(Duration::from_millis(200)), Outcome::NoBytes);
        conn.close();
        handle.join().expect("server thread");
    }

    /// Half-close: the server sees a clean end of stream (close_notify,
    /// then TCP EOF) after the data sent before it, and can still reply;
    /// the client reads the reply and then sees the server's close.
    #[test]
    fn tls_half_close_delivers_clean_eof_and_reads_continue() {
        let (mut conn, handle, _m) = tls_pair(|mut s| {
            let (data, clean) = server_read_to_end(&mut s);
            assert_eq!(data, b"reserve\r\n");
            assert!(clean, "half-close must send close_notify");
            // The TCP write side is shut down as well: raw EOF follows.
            let mut raw = [0u8; 16];
            assert_eq!(s.sock.read(&mut raw).expect("raw read"), 0);
            s.write_all(b"TIMED_OUT\r\n").expect("write");
            s.flush().expect("flush");
            server_close_notify(&mut s);
        });
        conn.send(b"reserve\r\n").expect("send");
        assert_eq!(conn.shutdown_write(), Outcome::ShutdownDone);
        assert_eq!(
            conn.recv_response(),
            Outcome::Received(b"TIMED_OUT\r\n".to_vec())
        );
        assert_eq!(
            conn.recv_closed(Duration::from_millis(1000)),
            Outcome::Closed
        );
        // Writing after the half-close is an I/O error, as over TCP.
        assert!(matches!(conn.send(b"x"), Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe));
        handle.join().expect("server thread");
    }

    /// A close_notify from the server is a close even while its TCP
    /// connection stays open.
    #[test]
    fn tls_recv_closed_after_server_close_notify() {
        let (mut conn, handle, _m) = tls_pair(|mut s| {
            server_close_notify(&mut s);
            // Keep the socket open well past the client's window start.
            std::thread::sleep(Duration::from_millis(500));
        });
        assert_eq!(
            conn.recv_closed(Duration::from_millis(1000)),
            Outcome::Closed
        );
        assert_eq!(conn.recv_response(), Outcome::Timeout);
        handle.join().expect("server thread");
    }

    /// A bare TCP close (no close_notify) is reported as closed too, after
    /// any data that preceded it.
    #[test]
    fn tls_recv_closed_after_tcp_eof_without_close_notify() {
        let (mut conn, handle, _m) = tls_pair(|mut s| {
            s.write_all(b"BYE\r\n").expect("write");
            s.flush().expect("flush");
            s.sock.shutdown(Shutdown::Both).expect("shutdown");
        });
        handle.join().expect("server thread");
        assert_eq!(conn.recv_response(), Outcome::Received(b"BYE\r\n".to_vec()));
        assert_eq!(
            conn.recv_closed(Duration::from_millis(1000)),
            Outcome::Closed
        );
    }

    /// `recv_closed` on a connection that stays open reports it open.
    #[test]
    fn tls_recv_closed_still_open() {
        let (mut conn, handle, _m) = tls_pair(|mut s| {
            let _ = server_read_to_end(&mut s);
        });
        assert_eq!(
            conn.recv_closed(Duration::from_millis(200)),
            Outcome::StillOpen
        );
        conn.close();
        handle.join().expect("server thread");
    }

    /// `close` ends the TLS session cleanly (close_notify).
    #[test]
    fn tls_close_sends_close_notify() {
        let (conn, handle, _m) = tls_pair(|mut s| {
            let (data, clean) = server_read_to_end(&mut s);
            assert!(data.is_empty());
            assert!(clean, "close must send close_notify");
        });
        conn.close();
        handle.join().expect("server thread");
    }

    /// TLS counterpart of the macOS SO_RCVTIMEO test above: data buffered
    /// after both sides shut down is still read, then the close.
    #[test]
    fn tls_buffered_data_is_read_after_both_sides_shut_down() {
        let (mut conn, handle, _m) = tls_pair(|mut s| {
            let _ = server_read_to_end(&mut s);
            s.write_all(b"ONE\r\n").expect("write");
            s.write_all(b"TWO\r\n").expect("write");
            s.flush().expect("flush");
            server_close_notify(&mut s);
        });
        assert_eq!(conn.shutdown_write(), Outcome::ShutdownDone);
        handle.join().expect("server thread");
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(conn.recv_response(), Outcome::Received(b"ONE\r\n".to_vec()));
        assert_eq!(conn.recv_response(), Outcome::Received(b"TWO\r\n".to_vec()));
        assert_eq!(
            conn.recv_closed(Duration::from_millis(200)),
            Outcome::Closed
        );
    }

    /// The client trusts only the harness's own CA.
    #[test]
    fn tls_rejects_server_with_untrusted_certificate() {
        let server_material = TlsMaterial::generate().expect("generate");
        let other = TlsMaterial::generate().expect("generate");
        let config = server_config(&server_material);
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let handle = std::thread::spawn(move || {
            let (sock, _) = listener.accept().expect("accept");
            let conn = ServerConnection::new(config).expect("server conn");
            let mut s = StreamOwned::new(conn, sock);
            while s.conn.is_handshaking() {
                if s.conn.complete_io(&mut s.sock).is_err() {
                    return;
                }
            }
        });
        let transport = ClientTransport::Tls(other.client_config());
        assert!(ConnHandle::connect(addr, &transport).is_err());
        handle.join().expect("server thread");
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
