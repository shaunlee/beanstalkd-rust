//! Per-connection task: reads and decodes the client's byte stream, sends
//! at most one command at a time to the engine actor, and writes back
//! whatever it replies. See docs/DESIGN.md §6 and the half-close notes in
//! `.ref/beanstalkd/prot.c` (`STATE_WAIT`, `halfclosed`, `h_conn`).

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc;
use tokio_util::codec::Decoder;

use bstk_engine::ConnId;
use bstk_proto::{Command, Frame, Response, ServerCodec};

use crate::engine_actor::{EngineHandle, EngineMsg};

/// Initial read-buffer capacity; grown by the codec itself for large put
/// bodies (see `ServerCodec`'s `PutBody` state).
const INITIAL_BUF_CAPACITY: usize = 4 * 1024;

/// While a command awaits its reply we keep reading only to notice EOF.
/// Past this many buffered bytes we stop reading, so a client that keeps
/// pipelining while blocked (e.g. in `reserve`) is held back by TCP flow
/// control instead of growing server memory without bound.
const MAX_BUFFERED_WHILE_WAITING: usize = 64 * 1024;

/// Sends `Disconnect` to the engine on every exit path of
/// [`handle_connection`] -- return, error, or panic unwinding through this
/// task -- per T4 requirement 5. Unbounded channels make `send` from `Drop`
/// safe (never blocks, never awaits).
struct DisconnectGuard {
    conn: ConnId,
    engine_tx: EngineHandle,
}

impl Drop for DisconnectGuard {
    fn drop(&mut self) {
        let _ = self
            .engine_tx
            .send(EngineMsg::Disconnect { conn: self.conn });
    }
}

/// Drives one client connection until it closes (EOF, `quit`, or an I/O
/// error). `reply_rx` receives replies the engine actor addressed to
/// `conn`; the caller must have already sent `EngineMsg::Connect` for this
/// connection (and must do so before spawning this task, to preserve
/// ordering on the shared engine channel).
pub async fn handle_connection(
    stream: TcpStream,
    conn: ConnId,
    engine_tx: EngineHandle,
    mut reply_rx: mpsc::UnboundedReceiver<Response>,
    max_job_size: u32,
) {
    let _guard = DisconnectGuard {
        conn,
        engine_tx: engine_tx.clone(),
    };
    let (mut rh, mut wh) = stream.into_split();
    let mut codec = ServerCodec::new(max_job_size).emit_put_started();
    let mut rbuf = BytesMut::with_capacity(INITIAL_BUF_CAPACITY);
    let mut wbuf = BytesMut::with_capacity(256);
    // Once true, the peer will never produce more bytes (EOF or a write
    // error was already observed). We keep serving any reply still owed to
    // an in-flight command, but stop attempting further reads.
    let mut eof = false;

    loop {
        match codec.decode(&mut rbuf) {
            Ok(Some(Frame::Command(Command::Quit))) => {
                // prot.c: `quit` closes the connection as soon as its
                // 4-byte prefix matches, discarding the rest of the line
                // and anything else already buffered. No reply is sent.
                let _ = flush(&mut wbuf, &mut wh).await;
                return;
            }
            Ok(Some(Frame::Command(cmd))) => {
                // Flush any batched direct-error replies before this
                // command reaches the engine: the reference never
                // dispatches a command until the previous reply has been
                // fully written to the socket.
                if flush(&mut wbuf, &mut wh).await.is_err() {
                    return;
                }
                if engine_tx.send(EngineMsg::Command { conn, cmd }).is_err() {
                    return;
                }
                if eof {
                    reassert_half_close(&engine_tx, conn);
                }
                match await_reply(
                    &mut reply_rx,
                    &mut rh,
                    &mut rbuf,
                    &mut eof,
                    &engine_tx,
                    conn,
                )
                .await
                {
                    Some(resp) => {
                        resp.encode(&mut wbuf);
                        if flush(&mut wbuf, &mut wh).await.is_err() {
                            return;
                        }
                    }
                    None => return,
                }
            }
            Ok(Some(Frame::PutRejected(why))) => {
                // Same ordering rationale as the `Frame::Command` arm above.
                if flush(&mut wbuf, &mut wh).await.is_err() {
                    return;
                }
                if engine_tx
                    .send(EngineMsg::PutRejected { conn, why })
                    .is_err()
                {
                    return;
                }
                if eof {
                    reassert_half_close(&engine_tx, conn);
                }
                match await_reply(
                    &mut reply_rx,
                    &mut rh,
                    &mut rbuf,
                    &mut eof,
                    &engine_tx,
                    conn,
                )
                .await
                {
                    Some(resp) => {
                        resp.encode(&mut wbuf);
                        if flush(&mut wbuf, &mut wh).await.is_err() {
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
            Ok(Some(Frame::Error(resp))) => {
                // No engine round trip needed; batch consecutive
                // decode-time errors into one write, flushed the next time
                // we would otherwise block (see the `Ok(None)` arm).
                resp.encode(&mut wbuf);
            }
            Ok(None) => {
                if flush(&mut wbuf, &mut wh).await.is_err() {
                    return;
                }
                if eof {
                    // Nothing decodable is left buffered, and the peer will
                    // never send more: this is a real close.
                    return;
                }
                match rh.read_buf(&mut rbuf).await {
                    Ok(0) => eof = true,
                    Ok(_) => {}
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
/// socket in the meantime (to detect EOF/half-close per T4 requirement 6),
/// but never decodes or dispatches another command while a reply is
/// outstanding -- at most one command is ever in flight per connection.
async fn await_reply(
    reply_rx: &mut mpsc::UnboundedReceiver<Response>,
    rh: &mut OwnedReadHalf,
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
            res = rh.read_buf(rbuf) => match res {
                Ok(0) => {
                    *eof = true;
                    // Mirrors `halfclosed` + `STATE_WAIT` in prot.c: this is
                    // a no-op unless `conn` is actually blocked in reserve,
                    // in which case it produces TIMED_OUT.
                    reassert_half_close(engine_tx, conn);
                }
                Ok(_) => {
                    // More pipelined bytes arrived; leave them buffered.
                    // They are only decoded once this reply has arrived.
                }
                Err(_) => {
                    *eof = true;
                    reassert_half_close(engine_tx, conn);
                }
            },
        }
    }
}

/// Writes and clears `wbuf` if non-empty. A write error means the
/// connection is dead; the caller treats that the same as EOF and closes.
async fn flush(wbuf: &mut BytesMut, wh: &mut OwnedWriteHalf) -> std::io::Result<()> {
    if wbuf.is_empty() {
        return Ok(());
    }
    let res = wh.write_all(wbuf).await;
    wbuf.clear();
    res
}
