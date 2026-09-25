//! The cluster actor: consumes the connection tasks' `EngineMsg`s and
//! sends them, in order, to the leader.
//!
//! # Ordering
//!
//! The state machine accepts a `Connect` only if its local number is above
//! every earlier `Connect` of the same node, and any other input only at
//! its connection's exact next `seq`. So this node's inputs must reach the
//! log in the order they were generated. They all go through one queue
//! ([`ForwardQueue`]):
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
//! item not yet applied is sent again in its original order, only when
//! there is a reason to believe something was lost:
//! - the leader or the term changes (openraft metrics), which also abandons
//!   a forward in flight;
//! - the leader answers `NotLeader`, or a forward fails (then after a short
//!   pause);
//! - an item was applied while an item sent before it in the same pass
//!   was not: entries of one pass reach the log in order, so the earlier
//!   one was dropped (for example a leader lost and regained leadership
//!   between two batches without a visible view change);
//! - as a last resort, the oldest item has not been applied for the stall
//!   timeout since it was last sent. The timeout starts at
//!   [`STALL_RESEND`] and doubles after every stall resend up to
//!   [`STALL_RESEND_MAX`] (reset when the oldest item is applied or the
//!   view changes), so that an overloaded leader, which is merely slow,
//!   does not get the whole queue again every few seconds.
//!
//! The state machine ignores the duplicates this produces (counted in
//! `resent_items`). Before such a resend, items that can no longer apply
//! are pruned (their connection's `Connect` is settled and the connection
//! is gone, or their `seq` is already applied), and a local connection that
//! the state no longer has is closed.
//!
//! # Bounds
//!
//! The queue holds at most [`MAX_QUEUE_ITEMS`] items and about
//! [`MAX_QUEUE_BYTES`] bytes. At the bound, new client connections are
//! refused at accept (`Clients::admit`) and a put is answered
//! `OUT_OF_MEMORY` (its body is replaced by a replicated
//! `PutRejected(OutOfMemory)`, which is what a reference server says when
//! its binlog cannot take the job). Other inputs of open connections are
//! still queued: each connection has at most one command in flight, so
//! they are bounded by the number of connections.
//!
//! # Disconnect
//!
//! When a connection task ends (the client went away, or this node closed
//! the socket: `ReplySink::closed`, isolation, shutdown), its guard sends
//! `Disconnect`. It is queued unless the state has settled the connection's
//! `Connect` and no longer has the connection (it was dropped already), or
//! the connection was never queued (refused while isolated).
//!
//! # Isolation
//!
//! The actor tracks when the leader last accepted something from this
//! node (`last_leader_ok`: a forward, or a *ping*, an empty forward sent
//! every [`ping_interval`] when there is nothing else to send), which a
//! rewind does not reset. The node is cut off when:
//! - it follows a leader that has not accepted anything from it for
//!   `node_timeout` (this also catches a one-way partition, where the
//!   leader's replication still reaches this node but nothing gets back);
//! - or no leader is known, or this node leads without a quorum
//!   acknowledgement, for `node_timeout`.
//!
//! Then every client socket is closed, and so is any new connection (at
//! accept, before it is numbered or queued) until a leader is reachable
//! again. Their `Disconnect`s are sent once it is.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};

use bstk_engine::{ConnId, EngineInput};
use bstk_proto::{Command, PutRejection};
use bstk_raft::forward::ForwardError;
use bstk_raft::{ForwardRequest, ForwardResponse, NodeId, Op};

use super::{ControlOutcome, Core, Event, local_of};
use crate::engine_actor::EngineMsg;

/// Initial stall timeout: resend everything unapplied if the oldest item
/// waits this long after it was sent.
pub const STALL_RESEND: Duration = Duration::from_secs(2);
/// Largest stall timeout (after repeated stall resends).
pub const STALL_RESEND_MAX: Duration = Duration::from_secs(30);
/// Pause after a failed forward before trying again.
const RETRY_PAUSE: Duration = Duration::from_millis(50);
/// Limits of one `ForwardRequest` (at least one item is always sent).
const MAX_BATCH_ITEMS: usize = 1024;
const MAX_BATCH_BYTES: usize = 4 << 20;
/// Bounds of the forward queue (see the module docs).
pub const MAX_QUEUE_ITEMS: usize = 100_000;
pub const MAX_QUEUE_BYTES: usize = 128 << 20;
/// Housekeeping period (resend, isolation, shutdown).
const TICK: Duration = Duration::from_millis(50);
/// Messages handled per wake-up before the queue is pumped.
const MAX_DRAIN: usize = 1024;

/// How often an owner with nothing to forward pings the leader: a quarter
/// of `node_timeout`, between 50 ms and 1 s.
pub fn ping_interval(node_timeout: Duration) -> Duration {
    (node_timeout / 4).clamp(Duration::from_millis(50), Duration::from_secs(1))
}

#[derive(Debug)]
struct Item {
    conn: ConnId,
    seq: u64,
    input: EngineInput,
    /// Approximate encoded size (for batching and the byte bound).
    size: usize,
    /// Times sent (or proposed).
    sends: u32,
    /// The pass it was last sent in.
    pass: u64,
}

/// What [`ForwardQueue::applied`] found.
#[derive(Debug, Default, PartialEq, Eq)]
struct AppliedOutcome {
    /// The item was queued (and is now removed).
    found: bool,
    /// It was the oldest item.
    front: bool,
    /// An item sent before it in the same pass is still unapplied: that
    /// one was dropped.
    dropped_before: bool,
}

/// The ordered queue of this node's unapplied inputs and the send cursor.
/// A *pass* is one run of the cursor from the front; every rewind starts a
/// new pass.
#[derive(Default)]
struct ForwardQueue {
    items: VecDeque<Item>,
    /// Items before it have been sent in the current pass.
    cursor: usize,
    pass: u64,
    bytes: usize,
    /// Items sent again after an earlier send.
    resent: u64,
}

impl ForwardQueue {
    fn len(&self) -> usize {
        self.items.len()
    }

    fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// At (or beyond) one of the bounds.
    fn full(&self) -> bool {
        self.items.len() >= MAX_QUEUE_ITEMS || self.bytes >= MAX_QUEUE_BYTES
    }

    fn has_unsent(&self) -> bool {
        self.cursor < self.items.len()
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
        self.bytes += size;
        self.items.push_back(Item {
            conn,
            seq,
            input,
            size,
            sends: 0,
            pass: 0,
        });
    }

    fn rewind(&mut self) {
        self.cursor = 0;
        self.pass += 1;
    }

    /// The next unsent item, marked as sent in the current pass.
    fn send_next(&mut self) -> Option<&Item> {
        let pass = self.pass;
        let item = self.items.get_mut(self.cursor)?;
        if item.sends > 0 {
            self.resent += 1;
        }
        item.sends = item.sends.saturating_add(1);
        item.pass = pass;
        self.cursor += 1;
        Some(item)
    }

    fn peek_unsent(&self) -> Option<&Item> {
        self.items.get(self.cursor)
    }

    /// Input `seq` of `conn` was applied.
    fn applied(&mut self, conn: ConnId, seq: u64) -> AppliedOutcome {
        let pos = match self.items.front() {
            Some(f) if f.conn == conn && f.seq == seq => Some(0),
            _ => self
                .items
                .iter()
                .position(|i| i.conn == conn && i.seq == seq),
        };
        let Some(pos) = pos else {
            return AppliedOutcome::default();
        };
        let item = &self.items[pos];
        // Only an item sent exactly once is known to have been applied from
        // that pass's copy.
        let dropped_before = pos > 0
            && item.sends == 1
            && self
                .items
                .iter()
                .take(pos)
                .any(|j| j.sends > 0 && j.pass == item.pass);
        if let Some(item) = self.items.remove(pos) {
            self.bytes -= item.size;
        }
        if pos < self.cursor {
            self.cursor -= 1;
        }
        AppliedOutcome {
            found: true,
            front: pos == 0,
            dropped_before,
        }
    }

    /// Keeps only the items `keep` accepts (the cursor is reset: callers
    /// rewind right after).
    fn retain(&mut self, mut keep: impl FnMut(&Item) -> bool) {
        let mut bytes = self.bytes;
        self.items.retain(|i| {
            let k = keep(i);
            if !k {
                bytes -= i.size;
            }
            k
        });
        self.bytes = bytes;
        self.cursor = 0;
    }
}

type Inflight =
    Pin<Box<dyn Future<Output = (NodeId, Result<ForwardResponse, ForwardError>)> + Send>>;

pub struct Actor {
    core: Arc<Core>,
    /// Next `seq` of every open local connection.
    seqs: HashMap<ConnId, u64>,
    queue: ForwardQueue,
    /// When the front item was last (re)sent, or became the front.
    front_since: Instant,
    /// Current stall timeout (see the module docs).
    stall_after: Duration,
    retry_at: Option<Instant>,
    /// A leader named by a `NotLeader` answer (until the view changes).
    hint: Option<NodeId>,
    view: (Option<NodeId>, u64),
    /// Last time a leader accepted a forward or ping from this node.
    last_leader_ok: Instant,
    last_ping: Option<Instant>,
    ping_every: Duration,
    /// Since when no leader is known, or this node leads without a quorum.
    unreachable_since: Option<Instant>,
    isolated: bool,
    was_started: bool,
    /// Last resend caused by an out-of-order apply.
    last_drop_resend: Option<Instant>,
    shutdown: Option<(Instant, oneshot::Sender<()>)>,
}

impl Actor {
    pub fn new(core: Arc<Core>) -> Actor {
        let view = core.view();
        let ping_every = ping_interval(core.node_timeout);
        Actor {
            core,
            seqs: HashMap::new(),
            queue: ForwardQueue::default(),
            front_since: Instant::now(),
            stall_after: STALL_RESEND,
            retry_at: None,
            hint: None,
            view,
            last_leader_ok: Instant::now(),
            last_ping: None,
            ping_every,
            unreachable_since: None,
            isolated: false,
            was_started: false,
            last_drop_resend: None,
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
            self.publish();
        }
    }

    /// Updates the shared status and the admission of new clients.
    fn publish(&self) {
        let st = &self.core.status;
        let full = self.queue.full();
        st.queue_len
            .store(self.queue.len() as u64, Ordering::Relaxed);
        st.queue_bytes
            .store(self.queue.bytes as u64, Ordering::Relaxed);
        st.queue_full.store(full, Ordering::Relaxed);
        st.resent_items.store(self.queue.resent, Ordering::Relaxed);
        self.core
            .clients
            .set_admitting(!(self.isolated || self.shutdown.is_some() || full));
    }

    // ------------------------------------------------------------ input

    fn on_message(&mut self, msg: EngineMsg) {
        match msg {
            EngineMsg::Connect { conn, reply_tx } => {
                if self.isolated || self.shutdown.is_some() {
                    // Nothing is queued for it: no state is created for a
                    // connection this node cannot serve, and its
                    // `Disconnect` is ignored (no `seqs` entry).
                    self.core.clients.close(conn);
                    return;
                }
                self.core.clients.insert(conn, reply_tx);
                self.seqs.insert(conn, 2);
                self.push(conn, 1, EngineInput::Connect(conn));
            }
            EngineMsg::Command { conn, cmd } => {
                if matches!(cmd, Command::Put { .. }) && self.queue.full() {
                    self.core
                        .status
                        .rejected_puts
                        .fetch_add(1, Ordering::Relaxed);
                    let why = PutRejection::OutOfMemory;
                    self.push_next(conn, EngineInput::PutRejected { conn, why });
                } else {
                    self.push_next(conn, EngineInput::Command { conn, cmd });
                }
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
        if self.queue.is_empty() {
            self.front_since = Instant::now();
        }
        self.queue.push(conn, seq, input);
    }

    fn on_event(&mut self, ev: Event) {
        let Event::Applied(conn, seq) = ev;
        let out = self.queue.applied(conn, seq);
        if out.front {
            self.front_since = Instant::now();
            self.stall_after = STALL_RESEND;
        }
        // At most one such resend per `RETRY_PAUSE` (belt and braces: the
        // prune below removes every item that can never apply, so a later
        // apply cannot keep pointing at the same stuck item).
        if out.dropped_before
            && self
                .last_drop_resend
                .is_none_or(|t| t.elapsed() >= RETRY_PAUSE)
        {
            self.last_drop_resend = Some(Instant::now());
            tracing::debug!(
                queued = self.queue.len(),
                "an input was applied before an earlier one: resending the unapplied ones"
            );
            self.prune();
            let core = self.core.clone();
            self.rewind(&core.status.rewinds_dropped);
        }
    }

    // ------------------------------------------------------------ sending

    fn rewind(&mut self, cause: &std::sync::atomic::AtomicU64) {
        if self.queue.cursor > 0 {
            cause.fetch_add(1, Ordering::Relaxed);
        }
        self.queue.rewind();
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
        self.stall_after = STALL_RESEND;
        let core = self.core.clone();
        self.rewind(&core.status.rewinds_view);
        true
    }

    fn on_forward_result(&mut self, target: NodeId, res: Result<ForwardResponse, ForwardError>) {
        let core = self.core.clone();
        match res {
            Ok(ForwardResponse::Accepted) => {
                // Only the leader accepts.
                self.last_leader_ok = Instant::now();
            }
            Ok(ForwardResponse::NotLeader { leader }) => {
                self.rewind(&core.status.rewinds_error);
                self.hint = leader.filter(|&l| l != self.core.id && l != target);
                if self.hint.is_none() {
                    self.retry_at = Some(Instant::now() + RETRY_PAUSE);
                }
            }
            Err(e) => {
                tracing::debug!(target_node = target, error = %e, "forward failed");
                self.rewind(&core.status.rewinds_error);
                self.hint = None;
                self.retry_at = Some(Instant::now() + RETRY_PAUSE);
            }
        }
    }

    /// Sends what is unsent, or a ping when due; returns a new forward in
    /// flight, if any.
    async fn pump(&mut self) -> Option<Inflight> {
        if let Some(at) = self.retry_at {
            if Instant::now() < at {
                return None;
            }
            self.retry_at = None;
        }
        let core = self.core.clone();
        if !self.queue.has_unsent() {
            return self.ping(&core);
        }
        if self.queue.cursor == 0 {
            self.front_since = Instant::now();
        }
        if core.is_leader() {
            while let Some(item) = self.queue.peek_unsent() {
                let op = Op::Conn {
                    seq: item.seq,
                    input: item.input.clone(),
                };
                if core.propose(op).await.is_err() {
                    // Raft has stopped.
                    return None;
                }
                self.queue.send_next();
            }
            return None;
        }
        let target = self
            .hint
            .or_else(|| core.leader())
            .filter(|&l| l != core.id)?;
        let mut items = Vec::new();
        let mut bytes = 0;
        while let Some(item) = self.queue.peek_unsent() {
            if !items.is_empty()
                && (items.len() >= MAX_BATCH_ITEMS || bytes + item.size > MAX_BATCH_BYTES)
            {
                break;
            }
            bytes += item.size;
            if let Some(item) = self.queue.send_next() {
                items.push((item.conn, item.seq, item.input.clone()));
            }
        }
        self.last_ping = Some(Instant::now());
        Some(forward(&core, target, items))
    }

    /// A ping (an empty forward) to the leader, when this node follows one
    /// and has not sent it anything for `ping_every`.
    fn ping(&mut self, core: &Arc<Core>) -> Option<Inflight> {
        let target = self
            .hint
            .or_else(|| core.leader())
            .filter(|&l| l != core.id)?;
        if self
            .last_ping
            .is_some_and(|t| t.elapsed() < self.ping_every)
        {
            return None;
        }
        self.last_ping = Some(Instant::now());
        Some(forward(core, target, Vec::new()))
    }

    // ------------------------------------------------------ housekeeping

    /// Returns true when the actor must stop (shutdown finished).
    fn on_tick(&mut self) -> bool {
        let now = Instant::now();
        if self.queue.cursor > 0 && now.duration_since(self.front_since) >= self.stall_after {
            self.prune();
            if !self.queue.is_empty() {
                tracing::debug!(
                    queued = self.queue.len(),
                    after = ?self.stall_after,
                    "inputs not applied in time; resending"
                );
                self.stall_after = (self.stall_after * 2).min(STALL_RESEND_MAX);
            }
            let core = self.core.clone();
            self.rewind(&core.status.rewinds_stall);
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
        let core = self.core.clone();
        if !core.status.started.load(Ordering::Acquire) {
            return;
        }
        if !self.was_started {
            // Startup may have taken long; count from now.
            self.was_started = true;
            self.last_leader_ok = now;
        }
        let cut_off_for = match core.leader() {
            Some(l) if l != core.id => {
                self.unreachable_since = None;
                now.duration_since(self.last_leader_ok)
            }
            _ if core.leader_reachable() => {
                // Leading with a quorum: should this node step down, its
                // time without a leader counts from now.
                self.unreachable_since = None;
                self.last_leader_ok = now;
                Duration::ZERO
            }
            _ => now.duration_since(*self.unreachable_since.get_or_insert(now)),
        };
        if cut_off_for < core.node_timeout {
            if self.isolated {
                tracing::warn!("a leader is reachable again; accepting clients");
                self.isolated = false;
                core.status.isolated.store(false, Ordering::Relaxed);
            }
            return;
        }
        if !self.isolated {
            tracing::warn!(
                timeout = ?core.node_timeout,
                "no leader reachable: closing every client connection"
            );
            self.isolated = true;
            core.status.isolated.store(true, Ordering::Relaxed);
            core.clients.set_admitting(false);
            core.clients.close_all();
        }
    }
}

fn forward(core: &Arc<Core>, target: NodeId, items: Vec<(ConnId, u64, EngineInput)>) -> Inflight {
    let req = ForwardRequest {
        from: core.id,
        items,
    };
    let net = core.net.clone();
    Box::pin(async move { (target, net.forward(target, req).await) })
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn connect(q: &mut ForwardQueue, conn: ConnId) {
        q.push(conn, 1, EngineInput::Connect(conn));
    }

    fn send_all(q: &mut ForwardQueue) -> Vec<(ConnId, u64)> {
        let mut out = Vec::new();
        while let Some(i) = q.send_next() {
            out.push((i.conn, i.seq));
        }
        out
    }

    #[test]
    fn in_order_applies_need_no_resend() {
        let mut q = ForwardQueue::default();
        for c in 1..=3 {
            connect(&mut q, c);
        }
        assert_eq!(send_all(&mut q), [(1, 1), (2, 1), (3, 1)]);
        for c in 1..=3 {
            let out = q.applied(c, 1);
            assert_eq!(
                out,
                AppliedOutcome {
                    found: true,
                    front: true,
                    dropped_before: false
                }
            );
        }
        assert!(q.is_empty());
        assert_eq!((q.bytes, q.cursor, q.resent), (0, 0, 0));
    }

    #[test]
    fn a_later_item_applied_first_reveals_a_drop() {
        let mut q = ForwardQueue::default();
        for c in 1..=3 {
            connect(&mut q, c);
        }
        send_all(&mut q);
        // Item 1 was lost; item 2 is applied.
        let out = q.applied(2, 1);
        assert!(out.found && !out.front && out.dropped_before, "{out:?}");
        assert_eq!(q.cursor, 2);
        // The resend of the remaining two counts as duplicates.
        q.rewind();
        assert_eq!(send_all(&mut q), [(1, 1), (3, 1)]);
        assert_eq!(q.resent, 2);
        // Item 3 was sent twice: applying it proves nothing about item 1's
        // latest copy.
        let out = q.applied(3, 1);
        assert!(out.found && !out.dropped_before, "{out:?}");
        // Not queued (a duplicate apply): nothing found.
        assert_eq!(q.applied(3, 1), AppliedOutcome::default());
    }

    #[test]
    fn items_of_a_newer_pass_prove_nothing_about_older_ones() {
        let mut q = ForwardQueue::default();
        connect(&mut q, 1);
        send_all(&mut q);
        // A view change: the queue is rewound but not yet resent; a new
        // item is sent in the new pass, the old one is not yet resent.
        q.rewind();
        connect(&mut q, 2);
        q.cursor = 1;
        send_all(&mut q);
        let out = q.applied(2, 1);
        assert!(out.found && !out.dropped_before, "{out:?}");
    }

    #[test]
    fn bounds_and_byte_accounting() {
        let mut q = ForwardQueue::default();
        let body = bytes::Bytes::from(vec![b'x'; MAX_QUEUE_BYTES / 2]);
        let put = |conn| EngineInput::Command {
            conn,
            cmd: Command::Put {
                pri: 0,
                delay: 0,
                ttr: 1,
                body: body.clone(),
            },
        };
        q.push(1, 2, put(1));
        assert!(!q.full());
        q.push(2, 2, put(2));
        assert!(q.full());
        assert_eq!(q.bytes, MAX_QUEUE_BYTES + 128);
        q.retain(|i| i.conn != 1);
        assert!(!q.full());
        assert_eq!(q.bytes, MAX_QUEUE_BYTES / 2 + 64);
        let mut q = ForwardQueue::default();
        for c in 0..MAX_QUEUE_ITEMS as u64 {
            connect(&mut q, c + 1);
        }
        assert!(q.full());
        assert_eq!(q.bytes, 64 * MAX_QUEUE_ITEMS);
    }

    #[test]
    fn ping_interval_is_clamped() {
        assert_eq!(
            ping_interval(Duration::from_secs(5)),
            Duration::from_secs(1)
        );
        assert_eq!(
            ping_interval(Duration::from_secs(1)),
            Duration::from_millis(250)
        );
        assert_eq!(
            ping_interval(Duration::from_millis(100)),
            Duration::from_millis(50)
        );
    }
}
