//! Per-connection task: reads and decodes the client's byte stream, sends at
//! most one command at a time to the engine actor, and writes back its replies
//! (docs/DESIGN.md §6; half-close: `STATE_WAIT`, `halfclosed`, `h_conn` in
//! prot.c).
//!
//! The protocol loop ([`command_loop`]) is generic over the stream type and
//! monomorphized for `QuietTcp` (plaintext listeners) and
//! `tokio_rustls::server::TlsStream<QuietTcp>` (TLS listeners), so plaintext
//! pays nothing for TLS. Reads and writes never overlap (a reply is written
//! only once no read is pending), so the stream is used whole, not split.
//!
//! The engine learns about a connection (`EngineMsg::Connect`) in the accept
//! loop for plaintext listeners, and for TLS listeners only after the
//! handshake and, with `auth = "token"`, a correct `auth <token>`: a failed
//! handshake or authentication never reaches the engine (no counter changes,
//! never in `stats`). [`ConnGuard`] sends the matching `Disconnect` on every
//! exit path, only if `Connect` was sent. TLS connections are *pending* (see
//! `pending`) until the handshake or authentication completes; a token
//! connection not authenticated within `auth.timeout` of its handshake is
//! closed without a reply.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bstk_net::QuietTcp;
use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_rustls::TlsAcceptor;
use tokio_util::codec::Decoder;

use bstk_engine::ConnId;
use bstk_proto::{Command, Frame, Response, ServerCodec};

use crate::auth::TokenSet;
use crate::cluster::Clients;
use crate::engine_actor::{EngineGone, EngineHandle, EngineMsg};
use crate::pending::{PendingGuard, ServerCounters};

/// Initial read-buffer capacity; grown by the codec itself for large put
/// bodies (see `ServerCodec`'s `PutBody` state).
const INITIAL_BUF_CAPACITY: usize = 4 * 1024;

/// While a command awaits its reply we keep reading only to notice EOF.
/// Past this many buffered bytes we stop reading, so a client that keeps
/// pipelining while blocked (e.g. in `reserve`) is held back by TCP flow
/// control instead of growing server memory without bound. Over TLS,
/// rustls buffers at most one more TLS record (16 KiB of plaintext) on top
/// of this: it stops reading the socket while it holds unread plaintext.
const MAX_BUFFERED_WHILE_WAITING: usize = 64 * 1024;

/// A TLS handshake must complete within this time, so a client that
/// connects and stalls cannot pin a task (and a socket) forever.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Upper bound on sending our `close_notify` when a TLS connection ends
/// (it normally completes at once; the peer may have stopped reading).
const CLOSE_NOTIFY_TIMEOUT: Duration = Duration::from_secs(1);

/// Sends `Disconnect` to the engine on every exit path of a connection
/// task -- return, error, or panic unwinding through it -- but only once
/// `Connect` was sent. Unbounded channels make `send` from `Drop` safe
/// (never blocks, never awaits).
pub struct ConnGuard {
    conn: Option<ConnId>,
    engine_tx: EngineHandle,
}

impl ConnGuard {
    pub fn connected(conn: ConnId, engine_tx: EngineHandle) -> ConnGuard {
        ConnGuard {
            conn: Some(conn),
            engine_tx,
        }
    }

    fn pending(engine_tx: EngineHandle) -> ConnGuard {
        ConnGuard {
            conn: None,
            engine_tx,
        }
    }

    fn connect(
        &mut self,
        conn: ConnId,
        reply_tx: mpsc::UnboundedSender<Response>,
    ) -> Result<(), EngineGone> {
        self.engine_tx.send(EngineMsg::Connect { conn, reply_tx })?;
        self.conn = Some(conn);
        Ok(())
    }
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        if let Some(conn) = self.conn {
            let _ = self.engine_tx.send(EngineMsg::Disconnect { conn });
        }
    }
}

#[derive(Clone, Copy)]
struct TokenAuth<'a> {
    tokens: &'a TokenSet,
    peer: SocketAddr,
    counters: &'a ServerCounters,
}

/// Drives one plaintext client connection until it closes (EOF, `quit` or an
/// I/O error). The caller must already have sent `EngineMsg::Connect` for it,
/// before spawning this task, to preserve ordering on the shared engine
/// channel.
pub async fn handle_plain(
    stream: TcpStream,
    conn: ConnId,
    engine_tx: EngineHandle,
    mut reply_rx: mpsc::UnboundedReceiver<Response>,
    max_job_size: u32,
) {
    let mut stream = match QuietTcp::new(stream) {
        Ok(s) => s,
        Err(e) => {
            tracing::debug!("cannot register a client socket: {e}");
            drop(ConnGuard::connected(conn, engine_tx));
            return;
        }
    };
    // Declared after the socket, so on every exit path `Disconnect` is
    // queued before the client can see the close (docs/DESIGN.md §6).
    let _guard = ConnGuard::connected(conn, engine_tx.clone());
    let mut codec = ServerCodec::new(max_job_size).emit_put_started();
    let mut rbuf = BytesMut::with_capacity(INITIAL_BUF_CAPACITY);
    let mut wbuf = BytesMut::with_capacity(256);
    command_loop(
        &mut stream,
        &mut codec,
        &mut rbuf,
        &mut wbuf,
        conn,
        &engine_tx,
        &mut reply_rx,
        None,
    )
    .await;
}

#[derive(Clone)]
pub enum TlsAuth {
    /// No authentication beyond the handshake (`auth = "none"`, and
    /// `auth = "mtls"`, where the acceptor requires a client certificate).
    Handshake,
    Token(Arc<TokenSet>),
}

pub enum ConnIds {
    Local(AtomicU64),
    Cluster(Arc<crate::cluster::durable::ConnIdBlocks>),
}

impl ConnIds {
    /// The next id, or `None` if none can be handed out (cluster mode: a
    /// new block could not be reserved); the connection is then closed.
    pub fn next(&self) -> Option<ConnId> {
        match self {
            ConnIds::Local(n) => Some(n.fetch_add(1, Ordering::Relaxed)),
            ConnIds::Cluster(blocks) => blocks.next(),
        }
    }
}

pub struct TlsListener {
    pub acceptor: TlsAcceptor,
    pub auth: TlsAuth,
    pub next_id: Arc<ConnIds>,
    pub engine_tx: EngineHandle,
    pub max_job_size: u32,
    pub counters: Arc<ServerCounters>,
    pub auth_timeout: Duration,
    pub clients: Option<Arc<Clients>>,
}

/// Drives one TLS client connection: handshake (bounded by
/// [`HANDSHAKE_TIMEOUT`]), token authentication if configured (bounded by
/// `auth.timeout`), then the same protocol loop as plaintext. The connection
/// counts as pending until the handshake or authentication succeeds, and its
/// id is allocated only once the engine is told about it.
pub async fn handle_tls(
    tcp: TcpStream,
    peer: SocketAddr,
    listener: Arc<TlsListener>,
    pending: PendingGuard,
) {
    let TlsListener {
        acceptor,
        auth,
        next_id,
        engine_tx,
        max_job_size,
        counters,
        auth_timeout,
        clients,
    } = &*listener;
    let tcp = match QuietTcp::new(tcp) {
        Ok(t) => t,
        Err(e) => {
            tracing::debug!(%peer, "cannot register a client socket: {e}");
            return;
        }
    };
    let mut stream = match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(tcp)).await {
        Ok(Ok(stream)) => stream,
        Ok(Err(e)) => {
            tracing::debug!(%peer, "TLS handshake failed: {e}");
            return;
        }
        Err(_) => {
            tracing::debug!(%peer, "TLS handshake timed out");
            return;
        }
    };

    let mut guard = ConnGuard::pending(engine_tx.clone());
    let mut codec = ServerCodec::new(*max_job_size).emit_put_started();
    let mut rbuf = BytesMut::with_capacity(INITIAL_BUF_CAPACITY);
    let mut wbuf = BytesMut::with_capacity(256);

    let token_auth = match auth {
        TlsAuth::Handshake => {
            drop(pending);
            None
        }
        TlsAuth::Token(tokens) => {
            codec = codec.recognize_auth();
            let ta = TokenAuth {
                tokens,
                peer,
                counters,
            };
            let authenticated = tokio::time::timeout(
                *auth_timeout,
                authenticate(&mut stream, &mut codec, &mut rbuf, &mut wbuf, ta),
            )
            .await;
            match authenticated {
                Ok(true) => {}
                Ok(false) => {
                    close_tls(&mut stream).await;
                    return;
                }
                Err(_) => {
                    counters.auth_timed_out();
                    tracing::debug!(%peer, "authentication timed out");
                    close_tls(&mut stream).await;
                    return;
                }
            }
            drop(pending);
            Some(ta)
        }
    };

    // Cluster mode: refused while the node is cut off, shutting down or
    // its forward queue is full (before an id is used).
    if clients.as_ref().is_some_and(|c| !c.admit()) {
        close_tls(&mut stream).await;
        return;
    }
    let Some(conn) = next_id.next() else {
        close_tls(&mut stream).await;
        return;
    };
    let (reply_tx, mut reply_rx) = mpsc::unbounded_channel();
    // Cluster mode: registered before `Connect`, so the cluster can close
    // the connection from its first reply on.
    let close = clients.as_ref().map(|c| c.register_closer(conn));
    // Sent by this task, before its first command on the same FIFO engine
    // channel, so the ordering the engine relies on holds.
    if guard.connect(conn, reply_tx).is_err() {
        tracing::error!("engine actor is gone; dropping new connection");
        return;
    }
    let serve = command_loop(
        &mut stream,
        &mut codec,
        &mut rbuf,
        &mut wbuf,
        conn,
        engine_tx,
        &mut reply_rx,
        token_auth,
    );
    match close {
        None => serve.await,
        Some(close) => {
            tokio::select! {
                () = serve => {}
                Ok(()) = close => {}
            }
        }
    }
    // The engine forgets the connection first (as when a plaintext socket
    // closes), then we end the TLS session cleanly.
    drop(guard);
    close_tls(&mut stream).await;
}

async fn close_tls<S: AsyncWrite + Unpin>(stream: &mut S) {
    let _ = tokio::time::timeout(CLOSE_NOTIFY_TIMEOUT, stream.shutdown()).await;
}

/// The unauthenticated phase of an `auth = "token"` connection. Returns
/// `true` once a correct `auth <token>` was received (`AUTHENTICATED` is
/// left in `wbuf`, flushed by the command loop with whatever follows);
/// `false` when the connection must be closed. Nothing here reaches the
/// engine:
/// - a wrong token gets `UNAUTHORIZED`;
/// - any other input (a command, the start of a `put`, a malformed line)
///   gets a single `UNAUTHORIZED`, before any `put` body is read;
/// - `quit` closes silently, as it always does;
/// - EOF or a read error closes.
///
/// Failures are logged at info with the peer address only; the token is
/// never logged.
async fn authenticate<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    codec: &mut ServerCodec,
    rbuf: &mut BytesMut,
    wbuf: &mut BytesMut,
    auth: TokenAuth<'_>,
) -> bool {
    loop {
        match codec.decode(rbuf) {
            Ok(Some(Frame::Auth(token))) => {
                if auth.tokens.verify(&token) {
                    tracing::debug!(peer = %auth.peer, "client authenticated");
                    Response::Authenticated.encode(wbuf);
                    return true;
                }
                tracing::info!(peer = %auth.peer, "authentication failed: wrong token");
                auth.counters.auth_failed();
                Response::Unauthorized.encode(wbuf);
                let _ = flush(wbuf, stream).await;
                return false;
            }
            Ok(Some(Frame::Command(Command::Quit))) => return false,
            Ok(Some(_)) => {
                tracing::info!(
                    peer = %auth.peer,
                    "authentication failed: command before authentication"
                );
                auth.counters.auth_failed();
                Response::Unauthorized.encode(wbuf);
                let _ = flush(wbuf, stream).await;
                return false;
            }
            Ok(None) => match stream.read_buf(rbuf).await {
                Ok(0) | Err(_) => return false,
                Ok(_) => {}
            },
            Err(_) => return false,
        }
    }
}

/// The protocol loop shared by every kind of connection. `auth` is set on
/// token-authenticated connections (already authenticated): a further
/// `auth <token>` is checked again; a correct one gets `AUTHENTICATED`
/// with no other effect, a wrong one `UNAUTHORIZED` and a close.
#[allow(clippy::too_many_arguments)]
async fn command_loop<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    codec: &mut ServerCodec,
    rbuf: &mut BytesMut,
    wbuf: &mut BytesMut,
    conn: ConnId,
    engine_tx: &EngineHandle,
    reply_rx: &mut mpsc::UnboundedReceiver<Response>,
    auth: Option<TokenAuth<'_>>,
) {
    // Once true, the peer will never produce more bytes (EOF or a read
    // error was already observed). We keep serving any reply still owed to
    // an in-flight command, but stop attempting further reads.
    let mut eof = false;

    loop {
        match codec.decode(rbuf) {
            Ok(Some(Frame::Command(Command::Quit))) => {
                // prot.c: `quit` closes the connection as soon as its
                // 4-byte prefix matches, discarding the rest of the line
                // and anything else already buffered. No reply is sent.
                let _ = flush(wbuf, stream).await;
                return;
            }
            Ok(Some(Frame::Command(cmd))) => {
                // Flush any batched direct-error replies before this
                // command reaches the engine: the reference never
                // dispatches a command until the previous reply has been
                // fully written to the socket.
                if flush(wbuf, stream).await.is_err() {
                    return;
                }
                if engine_tx.send(EngineMsg::Command { conn, cmd }).is_err() {
                    return;
                }
                if eof {
                    reassert_half_close(engine_tx, conn);
                }
                match await_reply(reply_rx, stream, rbuf, &mut eof, engine_tx, conn).await {
                    Some(resp) => {
                        resp.encode(wbuf);
                        if flush(wbuf, stream).await.is_err() {
                            return;
                        }
                    }
                    None => return,
                }
            }
            Ok(Some(Frame::PutRejected(why))) => {
                if flush(wbuf, stream).await.is_err() {
                    return;
                }
                if engine_tx
                    .send(EngineMsg::PutRejected { conn, why })
                    .is_err()
                {
                    return;
                }
                if eof {
                    reassert_half_close(engine_tx, conn);
                }
                match await_reply(reply_rx, stream, rbuf, &mut eof, engine_tx, conn).await {
                    Some(resp) => {
                        resp.encode(wbuf);
                        if flush(wbuf, stream).await.is_err() {
                            return;
                        }
                    }
                    None => return,
                }
            }
            Ok(Some(Frame::PutStarted { too_big })) => {
                // prot.c counts the put, marks the producer and allocates
                // the job id as soon as the command line parses, before the
                // body arrives. No reply, so nothing to wait for.
                if engine_tx
                    .send(EngineMsg::PutStarted { conn, too_big })
                    .is_err()
                {
                    return;
                }
            }
            Ok(Some(Frame::Auth(token))) => {
                let Some(auth) = auth else { return };
                if auth.tokens.verify(&token) {
                    Response::Authenticated.encode(wbuf);
                } else {
                    tracing::info!(peer = %auth.peer, "re-authentication failed: wrong token");
                    auth.counters.auth_failed();
                    Response::Unauthorized.encode(wbuf);
                    let _ = flush(wbuf, stream).await;
                    return;
                }
            }
            Ok(Some(Frame::Error(resp))) => {
                // No engine round trip needed; batch consecutive
                // decode-time errors into one write, flushed the next time
                // we would otherwise block (see the `Ok(None)` arm).
                resp.encode(wbuf);
            }
            Ok(None) => {
                if flush(wbuf, stream).await.is_err() {
                    return;
                }
                if eof {
                    return;
                }
                match stream.read_buf(rbuf).await {
                    Ok(0) => eof = true,
                    Ok(_) => {}
                    // TLS: the peer closed the TCP connection without a
                    // close_notify. It still is an end of stream: serve
                    // what is buffered, like a plaintext EOF. (Never
                    // returned by a plain TCP read.)
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => eof = true,
                    Err(_) => return,
                }
            }
            Err(_) => return,
        }
    }
}

/// Once EOF has already been observed, re-sends `HalfClose` right after
/// dispatching a fresh command: if that command is a reserve that only now
/// starts waiting, this guarantees it still receives TIMED_OUT (mirroring
/// `halfclosed` being sticky in prot.c across `dispatch_cmd` calls). The
/// engine's `half_close` is a no-op when the connection isn't waiting, so
/// this is always safe to send.
fn reassert_half_close(engine_tx: &EngineHandle, conn: ConnId) {
    let _ = engine_tx.send(EngineMsg::HalfClose { conn });
}

/// Waits for the reply to the command just dispatched. Keeps reading the
/// socket in the meantime (to detect EOF/half-close), but never decodes or
/// dispatches another command while a reply is outstanding -- at most one
/// command is ever in flight per connection. Over TLS a `close_notify`
/// reads as EOF too; we never shut down our own side, so the reply can
/// still be written afterwards.
async fn await_reply<S: AsyncRead + Unpin>(
    reply_rx: &mut mpsc::UnboundedReceiver<Response>,
    stream: &mut S,
    rbuf: &mut BytesMut,
    eof: &mut bool,
    engine_tx: &EngineHandle,
    conn: ConnId,
) -> Option<Response> {
    loop {
        if *eof || rbuf.len() >= MAX_BUFFERED_WHILE_WAITING {
            // Either the peer can never send more (polling `read_buf` again
            // would return `Ok(0)` immediately and busy-loop the select
            // below), or enough is buffered already. Only wait for the
            // engine now.
            return reply_rx.recv().await;
        }
        tokio::select! {
            reply = reply_rx.recv() => return reply,
            res = stream.read_buf(rbuf) => match res {
                Ok(0) => {
                    *eof = true;
                    // Mirrors `halfclosed` + `STATE_WAIT` in prot.c: this is
                    // a no-op unless `conn` is actually blocked in reserve,
                    // in which case it produces TIMED_OUT.
                    reassert_half_close(engine_tx, conn);
                }
                Ok(_) => {
                }
                Err(_) => {
                    *eof = true;
                    reassert_half_close(engine_tx, conn);
                }
            },
        }
    }
}

/// Writes and clears `wbuf` if non-empty, then flushes the stream (a no-op
/// for TCP; over TLS it pushes out every encrypted record, so a reply is
/// never left behind in rustls when the task returns). A write error
/// means the connection is dead; the caller treats that the same as EOF
/// and closes.
async fn flush<S: AsyncWrite + Unpin>(wbuf: &mut BytesMut, stream: &mut S) -> std::io::Result<()> {
    if wbuf.is_empty() {
        return Ok(());
    }
    let res = stream.write_all(wbuf).await;
    wbuf.clear();
    res?;
    stream.flush().await
}
