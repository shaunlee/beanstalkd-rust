//! The cluster actor: consumes the connection tasks' `EngineMsg`s and
//! sends them, in order, to the leader.
//!
//! # Ordering
//!
//! The state machine accepts a `Connect` only if its local number is above
//! every earlier `Connect` of the same node, and any other input only at
//! its connection's exact next `seq`. So this node's inputs must reach the
//! log in the order they were generated. They all go through one queue:
//!
//! - every message becomes an item `(conn, seq, input)` appended to the
//!   queue in arrival order (`seq` from a per-connection counter, 1 for
//!   `Connect`);
//! - one sender drains the queue from a cursor: as the leader, with
//!   `client_write_ff` per item (each call hands the entry to the Raft core
//!   before the next); otherwise as `ForwardRequest` batches to the leader,
//!   one in flight at a time (the leader proposes each batch in order
//!   before answering);
//! - an item leaves the queue only when the state machine reports it
//!   applied (`ReplySink::applied`); being accepted by the leader is not
//!   enough, as a leader can lose uncommitted entries.
//!
//! # Resend
//!
//! The cursor goes back to the front of the queue ("rewind"), so that every
//! item not yet applied is sent again in its original order, when:
//! - the leader or the term changes (openraft metrics), which also abandons
//!   a forward in flight;
//! - the leader answers `NotLeader`, or a forward fails (then after a short
//!   pause);
//! - the oldest item has not been applied [`STALL_RESEND`] after it was
//!   last sent (an entry lost without a visible leader change, for example
//!   when a leader lost and regained leadership between two batches).
//!
//! The state machine ignores the duplicates this produces. Before such a
//! stall resend, items that can no longer apply are pruned (their
//! connection's `Connect` is settled and the connection is gone, or their
//! `seq` is already applied), and a local connection that the state no
//! longer has is closed.
//!
//! # Disconnect
//!
//! When a connection task ends (the client went away, or this node closed
//! the socket: `ReplySink::closed`, isolation, shutdown), its guard sends
//! `Disconnect`. It is queued unless the state has settled the connection's
//! `Connect` and no longer has the connection (it was dropped already).
//!
//! # Isolation
//!
//! When no leader is reachable (none known, this node leads without a
//! recent quorum acknowledgement, or the oldest queued item has waited
//! longer) for `node_timeout`, every client socket is closed, and so is
//! any new connection until a leader is reachable again. Their
//! `Disconnect`s are sent once it is.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};

use bstk_engine::{ConnId, EngineInput};
use bstk_proto::Command;
use bstk_raft::forward::ForwardError;
use bstk_raft::{ForwardRequest, ForwardResponse, NodeId, Op};

use super::{ControlOutcome, Core, Event, local_of};
use crate::engine_actor::EngineMsg;

/// Resend everything unapplied if the oldest item waits this long.
pub const STALL_RESEND: Duration = Duration::from_secs(2);
/// Pause after a failed forward before trying again.
const RETRY_PAUSE: Duration = Duration::from_millis(50);
/// Limits of one `ForwardRequest` (at least one item is always sent).
const MAX_BATCH_ITEMS: usize = 1024;
const MAX_BATCH_BYTES: usize = 4 << 20;
/// Housekeeping period (resend, isolation, shutdown).
const TICK: Duration = Duration::from_millis(50);
/// Messages handled per wake-up before the queue is pumped.
const MAX_DRAIN: usize = 1024;

struct Item {
    conn: ConnId,
    seq: u64,
    input: EngineInput,
    /// Approximate encoded size (for batching).
    size: usize,
}

type Inflight =
    Pin<Box<dyn Future<Output = (NodeId, Result<ForwardResponse, ForwardError>)> + Send>>;

pub struct Actor {
    core: Arc<Core>,
    /// Next `seq` of every open local connection.
    seqs: HashMap<ConnId, u64>,
    queue: VecDeque<Item>,
    /// Items before it have been sent since the last rewind.
    cursor: usize,
    /// When the front item was last (re)sent, or became the front.
    front_since: Instant,
    retry_at: Option<Instant>,
    /// A leader named by a `NotLeader` answer (until the view changes).
    hint: Option<NodeId>,
    view: (Option<NodeId>, u64),
    unreachable_since: Option<Instant>,
    isolated: bool,
    shutdown: Option<(Instant, oneshot::Sender<()>)>,
}

impl Actor {
    pub fn new(core: Arc<Core>) -> Actor {
        let view = core.view();
        Actor {
            core,
            seqs: HashMap::new(),
            queue: VecDeque::new(),
            cursor: 0,
            front_since: Instant::now(),
            retry_at: None,
            hint: None,
            view,
            unreachable_since: None,
            isolated: false,
            shutdown: None,
        }
    }

    pub async fn run(
        mut self,
        mut rx: mpsc::UnboundedReceiver<EngineMsg>,
        mut events: mpsc::UnboundedReceiver<Event>,
    ) {
        let mut metrics = self.core.raft.metrics();
        let mut tick = tokio::time::interval(TICK);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut inflight: Option<Inflight> = None;
        loop {
            tokio::select! {
                msg = rx.recv() => {
                    let Some(msg) = msg else { return };
                    self.on_message(msg);
                    for _ in 0..MAX_DRAIN {
                        match rx.try_recv() {
                            Ok(msg) => self.on_message(msg),
                            Err(_) => break,
                        }
                    }
                }
                Some(ev) = events.recv() => {
                    self.on_event(ev);
                    while let Ok(ev) = events.try_recv() {
                        self.on_event(ev);
                    }
                }
                (target, res) = async {
                    match inflight.as_mut() {
                        Some(f) => f.await,
                        None => std::future::pending().await,
                    }
                } => {
                    inflight = None;
                    self.on_forward_result(target, res);
                }
                changed = metrics.changed() => {
                    if changed.is_err() {
                        // Raft has stopped; nothing more can be sent.
                        return;
                    }
                    if self.on_view_change() {
                        inflight = None;
                    }
                }
                _ = tick.tick() => {
                    if self.on_tick() {
                        return;
                    }
                }
            }
            if inflight.is_none() {
                inflight = self.pump().await;
            }
            self.core
                .status
                .queue_len
                .store(self.queue.len() as u64, Ordering::Relaxed);
        }
    }

    // ------------------------------------------------------------ input

    fn on_message(&mut self, msg: EngineMsg) {
        match msg {
            EngineMsg::Connect { conn, reply_tx } => {
                self.core.clients.insert(conn, reply_tx);
                self.seqs.insert(conn, 2);
                self.push(conn, 1, EngineInput::Connect(conn));
                if self.isolated || self.shutdown.is_some() {
                    self.core.clients.close(conn);
                }
            }
            EngineMsg::Command { conn, cmd } => {
                self.push_next(conn, EngineInput::Command { conn, cmd });
            }
            EngineMsg::PutStarted { conn, too_big } => {
                self.push_next(conn, EngineInput::PutStarted { conn, too_big });
            }
            EngineMsg::PutRejected { conn, why } => {
                self.push_next(conn, EngineInput::PutRejected { conn, why });
            }
            EngineMsg::HalfClose { conn } => {
                self.push_next(conn, EngineInput::HalfClose(conn));
            }
            EngineMsg::Disconnect { conn } => {
                self.core.clients.remove(conn);
                let Some(seq) = self.seqs.remove(&conn) else {
                    return;
                };
                if self.gone(conn) {
                    return;
                }
                self.push(conn, seq, EngineInput::Disconnect(conn));
            }
            EngineMsg::SetDraining(on) => {
                let core = self.core.clone();
                tokio::spawn(set_draining(core, on));
            }
            EngineMsg::Snapshot { max_tubes, reply } => {
                let now = self.core.monitoring_now();
                if let Some(s) = self.core.state.snapshot_limited(now, max_tubes) {
                    let _ = reply.send(s);
                }
            }
            EngineMsg::Shutdown { done } => {
                self.core.clients.close_all();
                self.shutdown = Some((Instant::now() + super::SHUTDOWN_BOUND, done));
            }
        }
    }

    /// Whether the replicated state has settled `conn`'s `Connect` and no
    /// longer has the connection.
    fn gone(&self, conn: ConnId) -> bool {
        let st = &self.core.state;
        local_of(conn) <= st.highest_local(self.core.id) && st.applied_seq(conn).is_none()
    }

    fn push_next(&mut self, conn: ConnId, input: EngineInput) {
        let Some(next) = self.seqs.get_mut(&conn) else {
            return;
        };
        let seq = *next;
        *next += 1;
        self.push(conn, seq, input);
    }

    fn push(&mut self, conn: ConnId, seq: u64, input: EngineInput) {
        let size = 64
            + match &input {
                EngineInput::Command {
                    cmd: Command::Put { body, .. },
                    ..
                } => body.len(),
                _ => 0,
            };
        if self.queue.is_empty() {
            self.front_since = Instant::now();
        }
        self.queue.push_back(Item {
            conn,
            seq,
            input,
            size,
        });
    }

    fn on_event(&mut self, ev: Event) {
        let Event::Applied(conn, seq) = ev;
        let pos = match self.queue.front() {
            Some(f) if f.conn == conn && f.seq == seq => Some(0),
            _ => self
                .queue
                .iter()
                .position(|i| i.conn == conn && i.seq == seq),
        };
        if let Some(pos) = pos {
            self.queue.remove(pos);
            if pos < self.cursor {
                self.cursor -= 1;
            }
            if pos == 0 {
                self.front_since = Instant::now();
            }
        }
    }

    // ------------------------------------------------------------ sending

    fn rewind(&mut self) {
        self.cursor = 0;
        self.front_since = Instant::now();
    }

    /// Returns whether a forward in flight must be abandoned.
    fn on_view_change(&mut self) -> bool {
        let view = self.core.view();
        if view == self.view {
            return false;
        }
        tracing::debug!(?view, "cluster view changed; resending unapplied inputs");
        self.view = view;
        self.hint = None;
        self.retry_at = None;
        self.rewind();
        true
    }

    fn on_forward_result(&mut self, target: NodeId, res: Result<ForwardResponse, ForwardError>) {
        match res {
            Ok(ForwardResponse::Accepted) => {}
            Ok(ForwardResponse::NotLeader { leader }) => {
                self.rewind();
                self.hint = leader.filter(|&l| l != self.core.id && l != target);
                if self.hint.is_none() {
                    self.retry_at = Some(Instant::now() + RETRY_PAUSE);
                }
            }
            Err(e) => {
                tracing::debug!(target_node = target, error = %e, "forward failed");
                self.rewind();
                self.hint = None;
                self.retry_at = Some(Instant::now() + RETRY_PAUSE);
            }
        }
    }

    /// Sends what is unsent; returns a new forward in flight, if any.
    async fn pump(&mut self) -> Option<Inflight> {
        if self.cursor >= self.queue.len() {
            return None;
        }
        if let Some(at) = self.retry_at {
            if Instant::now() < at {
                return None;
            }
            self.retry_at = None;
        }
        if self.cursor == 0 {
            self.front_since = Instant::now();
        }
        let core = self.core.clone();
        if core.is_leader() {
            while let Some(item) = self.queue.get(self.cursor) {
                let op = Op::Conn {
                    seq: item.seq,
                    input: item.input.clone(),
                };
                if core.propose(op).await.is_err() {
                    // Raft has stopped.
                    return None;
                }
                self.cursor += 1;
            }
            return None;
        }
        let target = self
            .hint
            .or_else(|| core.leader())
            .filter(|&l| l != core.id)?;
        let mut items = Vec::new();
        let mut bytes = 0;
        while let Some(item) = self.queue.get(self.cursor) {
            if !items.is_empty()
                && (items.len() >= MAX_BATCH_ITEMS || bytes + item.size > MAX_BATCH_BYTES)
            {
                break;
            }
            bytes += item.size;
            items.push((item.conn, item.seq, item.input.clone()));
            self.cursor += 1;
        }
        let req = ForwardRequest {
            from: core.id,
            items,
        };
        let net = core.net.clone();
        Some(Box::pin(
            async move { (target, net.forward(target, req).await) },
        ))
    }

    // ------------------------------------------------------ housekeeping

    /// Returns true when the actor must stop (shutdown finished).
    fn on_tick(&mut self) -> bool {
        let now = Instant::now();
        if self.cursor > 0 && now.duration_since(self.front_since) >= STALL_RESEND {
            self.prune();
            if !self.queue.is_empty() {
                tracing::debug!(
                    queued = self.queue.len(),
                    "inputs not applied in time; resending"
                );
            }
            self.rewind();
        }
        self.check_isolation(now);
        if let Some((deadline, _)) = &self.shutdown {
            let drained = self.core.clients.count() == 0 && self.queue.is_empty();
            if drained || now >= *deadline || !self.core.leader_reachable() {
                if let Some((_, done)) = self.shutdown.take() {
                    let _ = done.send(());
                }
                return true;
            }
        }
        false
    }

    /// Drops items that can no longer apply; closes local connections the
    /// state no longer has.
    fn prune(&mut self) {
        let st = &self.core.state;
        let highest = st.highest_local(self.core.id);
        let mut orphans = Vec::new();
        self.queue.retain(|item| {
            if local_of(item.conn) > highest {
                return true;
            }
            match st.applied_seq(item.conn) {
                Some(a) => a < item.seq,
                None => {
                    orphans.push(item.conn);
                    false
                }
            }
        });
        self.cursor = 0;
        for conn in orphans {
            if self.core.clients.holds(conn) {
                tracing::warn!(
                    conn,
                    "connection lost from the replicated state; closing it"
                );
                self.core.clients.close(conn);
            }
        }
    }

    fn check_isolation(&mut self, now: Instant) {
        let core = &self.core;
        if !core.status.started.load(Ordering::Acquire) {
            return;
        }
        let stalled = self.cursor > 0
            && !self.queue.is_empty()
            && now.duration_since(self.front_since) >= core.node_timeout;
        if core.leader_reachable() && !stalled {
            self.unreachable_since = None;
            if self.isolated {
                tracing::warn!("a leader is reachable again; accepting clients");
                self.isolated = false;
                core.status.isolated.store(false, Ordering::Relaxed);
            }
            return;
        }
        let since = *self.unreachable_since.get_or_insert(now);
        if !self.isolated && now.duration_since(since) >= core.node_timeout {
            tracing::warn!(
                timeout = ?core.node_timeout,
                "no leader reachable: closing every client connection"
            );
            self.isolated = true;
            core.status.isolated.store(true, Ordering::Relaxed);
            core.clients.close_all();
        }
    }
}

/// SIGUSR1: cluster-wide drain mode, retried until the leader has it.
async fn set_draining(core: Arc<Core>, on: bool) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        match core.control(Op::SetDraining(on)).await {
            ControlOutcome::Applied(_) => {
                tracing::warn!(on, "cluster-wide drain mode set");
                return;
            }
            // Setting drain mode twice is harmless: try again.
            ControlOutcome::Unknown | ControlOutcome::NotProposed => {}
        }
        if Instant::now() >= deadline {
            tracing::error!("could not reach a leader to set drain mode");
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
