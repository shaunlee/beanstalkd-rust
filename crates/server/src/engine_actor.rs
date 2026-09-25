//! The engine actor: the single tokio task that owns the `Engine` and is
//! its sole caller. All connection tasks talk to it through `EngineHandle`,
//! an unbounded channel of `EngineMsg`.
//!
//! Time discipline (see docs/PLAN.md T4 and docs/DESIGN.md §4.1): a single
//! `Instant` is captured at startup; `now` for every engine call is
//! `epoch.elapsed()` in nanoseconds, read fresh right before that call.
//! After every call (including the deadline-driven `tick`), we call
//! `tick(now)` again and recompute `next_deadline()` — this resolves the
//! DEADLINE_SOON-after-handle case documented in docs/COMPAT.md (engine
//! item 5).

use std::collections::HashMap;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::Instant as TokioInstant;

use bstk_engine::{ConnId, Engine, EngineConfig, Outbox, SysInfo};
use bstk_proto::{Command, PutRejection, Response};

/// A message sent by a connection task (or the signal handler) to the
/// engine actor.
pub enum EngineMsg {
    /// A new connection was accepted. Must be sent before that connection's
    /// first `Command`, on the same channel, so ordering is preserved.
    Connect {
        conn: ConnId,
        reply_tx: mpsc::UnboundedSender<Response>,
    },
    /// One fully-decoded command, sent only after the previous command on
    /// this connection has received its reply.
    Command { conn: ConnId, cmd: Command },
    /// A `put` rejected by the codec during framing (`Frame::PutRejected`).
    PutRejected { conn: ConnId, why: PutRejection },
    /// The connection's socket reached EOF while a reply was outstanding
    /// (or has already reached EOF and another command was just
    /// dispatched). A no-op unless the connection is currently blocked in
    /// reserve.
    HalfClose { conn: ConnId },
    /// The connection is gone; release its jobs and forget it. Sent from a
    /// drop guard so this fires on every exit path.
    Disconnect { conn: ConnId },
    /// SIGUSR1: enter (or, in principle, leave) drain mode.
    SetDraining(bool),
}

/// Handle used by connection tasks and the signal handlers to reach the
/// engine actor. Cheap to clone; sending never blocks (unbounded), so a
/// slow or wedged connection can never stall the engine.
pub type EngineHandle = mpsc::UnboundedSender<EngineMsg>;

/// Spawns the engine actor task and returns a handle to it. The task
/// captures its own time epoch (a single `Instant` for the actor's whole
/// lifetime) the moment it starts running.
pub fn spawn(cfg: EngineConfig, sys: Box<dyn SysInfo>) -> EngineHandle {
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(run(rx, cfg, sys));
    tx
}

async fn run(mut rx: mpsc::UnboundedReceiver<EngineMsg>, cfg: EngineConfig, sys: Box<dyn SysInfo>) {
    let epoch = std::time::Instant::now();
    let mut engine = Engine::new(0, cfg, sys);
    let mut conns: HashMap<ConnId, mpsc::UnboundedSender<Response>> = HashMap::new();
    let mut outbox: Outbox = Vec::new();

    loop {
        let deadline = engine.next_deadline();
        let sleep = async {
            match deadline {
                Some(nanos) => {
                    let target = tokio_deadline(epoch, nanos);
                    tokio::time::sleep_until(target).await;
                }
                None => std::future::pending::<()>().await,
            }
        };

        tokio::select! {
            msg = rx.recv() => {
                let Some(msg) = msg else {
                    // All senders dropped: nothing left to serve.
                    return;
                };
                let now = epoch.elapsed().as_nanos() as u64;
                outbox.clear();
                match msg {
                    EngineMsg::Connect { conn, reply_tx } => {
                        conns.insert(conn, reply_tx);
                        engine.connect(now, conn);
                    }
                    EngineMsg::Command { conn, cmd } => engine.handle(now, conn, cmd, &mut outbox),
                    EngineMsg::PutRejected { conn, why } => {
                        engine.put_rejected(now, conn, why, &mut outbox);
                    }
                    EngineMsg::HalfClose { conn } => engine.half_close(now, conn, &mut outbox),
                    EngineMsg::Disconnect { conn } => {
                        engine.disconnect(now, conn, &mut outbox);
                        conns.remove(&conn);
                    }
                    EngineMsg::SetDraining(on) => engine.set_draining(on),
                }
                // See docs/COMPAT.md engine item 5: an immediate tick after
                // every call resolves DEADLINE_SOON/TIMED_OUT decisions that
                // depend on time having "moved" past a boundary reached by
                // this very call.
                engine.tick(now, &mut outbox);
                deliver(&conns, &outbox);
            }
            () = sleep => {
                let now = epoch.elapsed().as_nanos() as u64;
                outbox.clear();
                engine.tick(now, &mut outbox);
                deliver(&conns, &outbox);
            }
        }
    }
}

/// `epoch + Duration::from_nanos(nanos)` as a `tokio::time::Instant`,
/// saturating instead of panicking: the engine can legitimately report a
/// deadline far enough in the future (e.g. a very long delay) that naive
/// addition would overflow `Instant`.
fn tokio_deadline(epoch: std::time::Instant, nanos: u64) -> TokioInstant {
    let std_target = epoch
        .checked_add(Duration::from_nanos(nanos))
        .unwrap_or_else(|| epoch + Duration::from_secs(u64::from(u32::MAX)));
    TokioInstant::from_std(std_target)
}

/// Delivers every reply in `outbox`, in order, to each connection's reply
/// channel. A missing or closed receiver (connection already gone) is
/// silently ignored: the engine must never block on, or fail because of, a
/// single connection.
fn deliver(conns: &HashMap<ConnId, mpsc::UnboundedSender<Response>>, outbox: &Outbox) {
    for (conn, resp) in outbox {
        if let Some(tx) = conns.get(conn) {
            let _ = tx.send(resp.clone());
        }
    }
}
