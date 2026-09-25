//! Cluster mode (`[cluster]`; docs/DESIGN.md §8, docs/PLAN.md §6.3): the
//! engine is replicated with Raft (bstk-raft on openraft 0.9) instead of
//! being owned by the local engine actor.
//!
//! # Architecture
//!
//! Connection tasks are unchanged: they talk to "the engine" through an
//! [`EngineHandle`] (`EngineHandle::Task`), exactly as in standalone mode.
//! In cluster mode the receiver of that channel is the cluster actor
//! ([`actor`]), which never runs the engine itself:
//!
//! ```text
//! conn tasks ──EngineMsg──▶ cluster actor ──ordered queue──▶ leader
//!                              │  (Op::Conn {seq, input})       │ client_write_ff
//!                              │                                ▼
//!                              │                        Raft log (majority)
//!                              │                                │ apply, on every node
//!   reply channels ◀──deliver── ReplySink (Clients) ◀──── ClusterStateMachine
//! ```
//!
//! - The actor numbers each connection's inputs (`seq`, from 1 with
//!   `Connect`) and appends them to one ordered queue. A single sender
//!   drains the queue to the current leader: directly with
//!   `client_write_ff` when this node leads, otherwise as batched
//!   `ForwardRequest`s, one in flight at a time. See [`actor`] for the
//!   ordering and resend rules.
//! - Every node applies every committed entry to its own engine
//!   (`ClusterStateMachine`), and [`Clients`] (the `ReplySink`) hands the
//!   replies for this node's connections to their reply channels.
//! - [`handler::Handler`] serves the cluster port's forwards and control
//!   requests (on the leader it proposes, elsewhere it answers
//!   `NotLeader`).
//! - [`duties`] runs the time-driven work: `Tick` proposals and node
//!   liveness (`DropNode`) on the leader, and readiness on every node.
//!
//! # Startup ([`start`])
//!
//! Open the storage (a locked data directory exits with status 10) and
//! start Raft with the TCP/TLS network. Then pick the startup mode:
//!
//! - **Bootstrap** (`--cluster-init`, empty data directory): first ask
//!   every reachable peer whether it already belongs to a running cluster
//!   (an empty `ForwardRequest`, see [`handler`]); if none does, initialize
//!   the membership from `[[cluster.peer]]` (every initial node is started
//!   this way, with the same peer list). If one does, bootstrapping would
//!   be wrong: the node joins in rejoin mode instead (with a warning).
//! - **Rejoin** (an empty data directory without `--cluster-init`, or the
//!   rejoin marker [`durable::REJOIN_FILE`] left by an unfinished rejoin):
//!   the node may have acknowledged entries in a previous life that it no
//!   longer has, so it must not help elect a leader until it has them
//!   again. It persists the marker, disables its elections and closes the
//!   vote gate of its cluster listener (inbound votes are refused), and
//!   serves no clients. It asks the leader to propose `DropNode(self)` and
//!   leaves rejoin mode once it has applied that entry: its index was
//!   learned from a leader after this process started, so everything
//!   committed before is now in this node's log. Leaving removes the
//!   marker durably, re-enables elections and opens the gate. A crash
//!   before that keeps the marker, so the node stays in rejoin mode.
//! - **Restart** (the data directory holds state): nothing special.
//!
//! Then start the cluster listener, wait until a leader is known and this
//! node has applied everything it knows to be committed, close out the
//! connections of this node's previous process with `DropNode(self)` and
//! wait until that is applied here. Only then are clients accepted, with
//! connection numbers from durably reserved blocks
//! ([`durable::ConnIdBlocks`]).

pub mod actor;
pub mod durable;
pub mod duties;
pub mod handler;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

use openraft::{BasicNode, Raft, RaftMetrics, ServerState, SnapshotPolicy};
use tokio::sync::{mpsc, oneshot, watch};

use bstk_engine::{ConnId, EngineConfig, Nanos};
use bstk_proto::Response;
use bstk_raft::client::{Network, NetworkConfig};
use bstk_raft::forward::{ControlRequest, ControlResponse, ForwardError};
use bstk_raft::listener::{ClusterListener, ListenerConfig, VoteGate};
use bstk_raft::storage::{self, LogOptions, LogStore, ReplySink, SmOptions, StateHandle};
use bstk_raft::tls::ClusterTls;
use bstk_raft::{
    CONN_SEQ_BITS, ForwardRequest, ForwardResponse, NodeId, Op, Request, TypeConfig, owner_of,
};

use crate::config::{ClusterSettings, SNAPSHOT_CHUNK};
use crate::engine_actor::{Clock, EngineHandle};
use crate::metrics::{ClusterInfo, ClusterStats};
use crate::sysinfo::{ProcessSysInfo, SharedSysInfo};

/// Used only when the data directory has no connection-number file
/// ([`durable::CONN_IDS_FILE`]) although the replicated state has seen
/// connections of this node (a node whose data directory was wiped): the
/// numbers its lost process handed out are unknown, so new ones start this
/// far above the highest one the state has seen, so that a `Connect` of
/// the lost process that commits late cannot make new connections look
/// old (the state machine accepts a `Connect` only above every earlier one
/// of the same node). With the file, new numbers start above everything
/// any earlier process may have used, and no gap is needed.
pub const CONN_ID_GAP: u64 = 1 << 20;

/// How long a leader waits for a control proposal to be applied before
/// answering without its index (the requester's timeout is longer).
const CONTROL_APPLY_BOUND: Duration = Duration::from_secs(1);

/// Upper bound on a shutdown's wait for the `Disconnect`s of the closed
/// client connections to be applied.
pub const SHUTDOWN_BOUND: Duration = Duration::from_secs(1);

type Metrics = RaftMetrics<NodeId, BasicNode>;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// The local number of a connection id.
pub fn local_of(conn: ConnId) -> u64 {
    conn & ((1 << CONN_SEQ_BITS) - 1)
}

/// What the actor learns from the state machine.
#[derive(Debug)]
pub enum Event {
    /// Input `seq` of local connection `conn` was applied.
    Applied(ConnId, u64),
}

/// This process's client connections: their reply channels and a way to
/// close their sockets. Shared by the accept loops (which register a
/// closer before sending `Connect`), the actor, and the state machine,
/// for which it is the [`ReplySink`].
pub struct Clients {
    replies: Mutex<HashMap<ConnId, mpsc::UnboundedSender<Response>>>,
    closers: Mutex<HashMap<ConnId, oneshot::Sender<()>>>,
    events: mpsc::UnboundedSender<Event>,
    /// Whether new client connections are admitted (set by the actor:
    /// false while isolated, shutting down, or the forward queue is full).
    admitting: AtomicBool,
    /// Client connections refused at accept because of that.
    refused: AtomicU64,
}

impl Clients {
    fn new(events: mpsc::UnboundedSender<Event>) -> Arc<Clients> {
        Arc::new(Clients {
            replies: Mutex::new(HashMap::new()),
            closers: Mutex::new(HashMap::new()),
            events,
            admitting: AtomicBool::new(true),
            refused: AtomicU64::new(0),
        })
    }

    /// Called by the accept loops before a new client connection gets an
    /// id: `false` means close it at once (it is counted).
    pub fn admit(&self) -> bool {
        if self.admitting.load(Ordering::Acquire) {
            return true;
        }
        self.refused.fetch_add(1, Ordering::Relaxed);
        false
    }

    fn set_admitting(&self, on: bool) {
        self.admitting.store(on, Ordering::Release);
    }

    /// Registers connection `conn` before its `Connect` is sent; the
    /// receiver completes when the cluster wants the socket closed.
    pub fn register_closer(&self, conn: ConnId) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        lock(&self.closers).insert(conn, tx);
        rx
    }

    /// Closes the socket of local connection `conn` (if still open).
    pub fn close(&self, conn: ConnId) {
        if let Some(tx) = lock(&self.closers).remove(&conn) {
            let _ = tx.send(());
        }
    }

    /// Closes every local client socket.
    pub fn close_all(&self) {
        let all: Vec<_> = lock(&self.closers).drain().collect();
        for (_, tx) in all {
            let _ = tx.send(());
        }
    }

    fn insert(&self, conn: ConnId, reply: mpsc::UnboundedSender<Response>) {
        lock(&self.replies).insert(conn, reply);
    }

    fn remove(&self, conn: ConnId) {
        lock(&self.replies).remove(&conn);
        lock(&self.closers).remove(&conn);
    }

    fn holds(&self, conn: ConnId) -> bool {
        lock(&self.replies).contains_key(&conn)
    }

    fn count(&self) -> usize {
        lock(&self.replies).len()
    }
}

impl ReplySink for Clients {
    fn applied(&self, conn: ConnId, seq: u64) {
        let _ = self.events.send(Event::Applied(conn, seq));
    }

    fn deliver(&self, conn: ConnId, resp: Response) {
        if let Some(tx) = lock(&self.replies).get(&conn) {
            let _ = tx.send(resp);
        }
    }

    fn closed(&self, conn: ConnId) {
        self.close(conn);
    }
}

/// Counters and flags shared by the cluster tasks (monitoring).
#[derive(Default)]
struct Status {
    /// The startup cleanup is done and clients are accepted.
    started: AtomicBool,
    /// `/readyz`: started, a leader is known, and the applied index has
    /// reached the last commit index this node learned.
    ready: AtomicBool,
    /// Client sockets are closed because no leader was reachable for
    /// `node_timeout`.
    isolated: AtomicBool,
    /// In rejoin mode (see the module docs).
    rejoining: AtomicBool,
    /// Last commit index learned (`u64::MAX`: none).
    committed: AtomicU64,
    /// Items in the forward queue, and their approximate size.
    queue_len: AtomicU64,
    queue_bytes: AtomicU64,
    /// The forward queue is at its bound (new clients refused, puts
    /// answered `OUT_OF_MEMORY`).
    queue_full: AtomicBool,
    /// Puts answered `OUT_OF_MEMORY` because the forward queue was full.
    rejected_puts: AtomicU64,
    /// Items sent again (to the leader, or proposed again as leader) after
    /// having been sent once: duplicates the state machine discards.
    resent_items: AtomicU64,
    /// Rewinds of the forward queue, by cause.
    rewinds_view: AtomicU64,
    rewinds_error: AtomicU64,
    rewinds_stall: AtomicU64,
    rewinds_dropped: AtomicU64,
    drop_node_proposals: AtomicU64,
}

/// Everything the cluster tasks share.
pub struct Core {
    id: NodeId,
    raft: Raft<TypeConfig>,
    metrics: watch::Receiver<Metrics>,
    state: StateHandle,
    net: Network,
    log: LogStore,
    data_dir: PathBuf,
    clock: Clock,
    /// Highest `now` this node has stamped on a proposal.
    stamped: AtomicU64,
    node_timeout: Duration,
    peers: BTreeSet<NodeId>,
    clients: Arc<Clients>,
    status: Status,
    /// Keeps `client_write_ff` receivers until they resolve (openraft
    /// logs a warning for every result it cannot deliver).
    reaper: mpsc::UnboundedSender<Pending>,
    /// Refuses inbound votes while in rejoin mode.
    gate: Arc<VoteGate>,
    /// When each peer last sent this node a forward, ping or control
    /// request (leader-side liveness, [`duties`]).
    heard: Mutex<HashMap<NodeId, Instant>>,
    /// Local connection numbers (set once clients are accepted).
    conn_ids: OnceLock<Arc<durable::ConnIdBlocks>>,
    /// `(when, bytes)` of the last snapshot-directory scan (`/metrics`).
    snapshot_size: Mutex<Option<(Instant, u64)>>,
}

/// How often `/metrics` rescans the snapshot directory at most.
const SNAPSHOT_SIZE_TTL: Duration = Duration::from_secs(3);

/// A proposal's result receiver, awaited by [`reap`] and discarded.
type Pending = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

/// Awaits proposal results nobody needs.
async fn reap(mut rx: mpsc::UnboundedReceiver<Pending>) {
    use futures::StreamExt;
    let mut pending = futures::stream::FuturesUnordered::new();
    loop {
        tokio::select! {
            f = rx.recv() => match f {
                Some(f) => pending.push(f),
                None => return,
            },
            Some(()) = pending.next(), if !pending.is_empty() => {}
        }
    }
}

/// Raft has stopped (fatal error or shutdown).
#[derive(Debug)]
struct RaftStopped;

/// The outcome of a control proposal.
#[derive(Debug)]
enum ControlOutcome {
    /// Applied on the leader at this index.
    Applied(u64),
    /// Proposed, or perhaps proposed: it may still commit later.
    Unknown,
    /// Not proposed (no leader, or not the leader).
    NotProposed,
}

impl Core {
    fn leader(&self) -> Option<NodeId> {
        self.metrics.borrow().current_leader
    }

    fn is_leader(&self) -> bool {
        let m = self.metrics.borrow();
        m.current_leader == Some(self.id) && m.state == ServerState::Leader
    }

    /// `(leader, term)` as this node sees them.
    fn view(&self) -> (Option<NodeId>, u64) {
        let m = self.metrics.borrow();
        (m.current_leader, m.current_term)
    }

    /// The `now` of a new proposal: never below this node's clock, the
    /// last applied `now`, or any `now` this node has already proposed.
    fn stamp(&self) -> Nanos {
        let now = self.clock.now().max(self.state.last_now());
        let prev = self.stamped.fetch_max(now, Ordering::AcqRel);
        prev.max(now)
    }

    /// Engine time for monitoring snapshots.
    fn monitoring_now(&self) -> Nanos {
        self.clock.now().max(self.state.last_now())
    }

    fn applied_index(&self) -> Option<u64> {
        self.state.last_applied().map(|l| l.index)
    }

    /// Proposes `op` without waiting for its commit (the leader only; on
    /// another node openraft refuses it once it reaches the Raft core).
    /// `Err` means Raft has stopped.
    async fn propose(&self, op: Op) -> Result<(), RaftStopped> {
        let req = Request {
            now: self.stamp(),
            op,
        };
        match self.raft.client_write_ff(req).await {
            Ok(rx) => {
                let _ = self.reaper.send(Box::pin(async move {
                    let _ = rx.await;
                }));
                Ok(())
            }
            Err(e) => {
                tracing::debug!("proposal refused: {e}");
                Err(RaftStopped)
            }
        }
    }

    /// Proposes `op` on this node (the leader) and waits, at most
    /// [`CONTROL_APPLY_BOUND`], for it to be applied here.
    async fn propose_and_wait(&self, op: Op) -> ControlOutcome {
        let req = Request {
            now: self.stamp(),
            op,
        };
        let mut rx = match self.raft.client_write_ff(req).await {
            Ok(rx) => rx,
            Err(_) => return ControlOutcome::NotProposed,
        };
        match tokio::time::timeout(CONTROL_APPLY_BOUND, &mut rx).await {
            Ok(Ok(Ok(resp))) => ControlOutcome::Applied(resp.log_id.index),
            // Refused (no longer the leader): it was not appended.
            Ok(Ok(Err(_))) => ControlOutcome::NotProposed,
            Ok(Err(_)) => ControlOutcome::Unknown,
            Err(_) => {
                let _ = self.reaper.send(Box::pin(async move {
                    let _ = rx.await;
                }));
                ControlOutcome::Unknown
            }
        }
    }

    /// Has the leader propose the control operation `op`.
    async fn control(&self, op: Op) -> ControlOutcome {
        match self.leader() {
            None => ControlOutcome::NotProposed,
            Some(l) if l == self.id => {
                if self.is_leader() {
                    self.propose_and_wait(op).await
                } else {
                    ControlOutcome::NotProposed
                }
            }
            Some(l) => {
                let req = ControlRequest { from: self.id, op };
                match self.net.control(l, req).await {
                    Ok(ControlResponse::Accepted { index: Some(i) }) => ControlOutcome::Applied(i),
                    Ok(ControlResponse::Accepted { index: None }) => ControlOutcome::Unknown,
                    Ok(ControlResponse::NotLeader { .. }) => ControlOutcome::NotProposed,
                    // Refused by the listener or never sent: not proposed.
                    Err(ForwardError::Rejected(_) | ForwardError::Unreachable(_)) => {
                        ControlOutcome::NotProposed
                    }
                    Err(ForwardError::Timeout | ForwardError::Network(_)) => {
                        ControlOutcome::Unknown
                    }
                }
            }
        }
    }

    /// Whether this node owns connections in the replicated state.
    fn owns_connections(&self, node: NodeId) -> bool {
        self.state.conn_ids().iter().any(|&c| owner_of(c) == node)
    }

    /// Waits until a leader is known and this node has applied everything
    /// it knows to be committed.
    async fn caught_up(&self) {
        loop {
            if self.leader().is_some() {
                let committed = self
                    .raft
                    .with_raft_state(|st| st.committed.map(|c| c.index))
                    .await
                    .unwrap_or(None);
                if committed.is_none_or(|c| self.applied_index().is_some_and(|a| a >= c)) {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Waits until this node has applied `index`.
    async fn applied_up_to(&self, index: u64) {
        let mut rx = self.state.subscribe();
        loop {
            if self.applied_index().is_some_and(|a| a >= index) {
                return;
            }
            let _ = tokio::time::timeout(Duration::from_millis(100), rx.changed()).await;
        }
    }

    /// The startup cleanup: `DropNode(self)` applied here, so that no
    /// connection of this node's previous process is left in the state
    /// (their reservations would otherwise stay held). Its bound is the
    /// `highest_local(self)` observed now; new connections are numbered
    /// above it, so neither this proposal nor a leader's for the old
    /// process can reach them, however late it commits.
    async fn drop_previous_connections(&self) {
        loop {
            self.caught_up().await;
            let op = Op::DropNode {
                node: self.id,
                up_to_local: self.state.highest_local(self.id),
            };
            match self.control(op).await {
                ControlOutcome::Applied(i) => {
                    self.applied_up_to(i).await;
                    tracing::info!(
                        index = i,
                        "connections of the previous process closed out (DropNode)"
                    );
                    return;
                }
                ControlOutcome::NotProposed => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                ControlOutcome::Unknown => {
                    // The proposal may still commit. Give it time (or a
                    // leader change) to settle, then check whether anything
                    // is left to close out. (A late duplicate is harmless:
                    // its bound is below every new connection.)
                    let (_, term) = self.view();
                    let deadline = Instant::now() + Duration::from_secs(2);
                    while Instant::now() < deadline && self.view().1 == term {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    self.caught_up().await;
                    if !self.owns_connections(self.id) {
                        return;
                    }
                }
            }
        }
    }

    /// Reachability as the actor sees it: a leader is known and, if it is
    /// this node, a quorum acknowledged it within `node_timeout`.
    fn leader_reachable(&self) -> bool {
        let m = self.metrics.borrow();
        match m.current_leader {
            None => false,
            Some(l) if l == self.id => {
                self.peers.len() == 1
                    || m.millis_since_quorum_ack
                        .is_some_and(|ms| u128::from(ms) <= self.node_timeout.as_millis())
            }
            Some(_) => true,
        }
    }

    /// Size of the stored snapshots, rescanned at most every
    /// [`SNAPSHOT_SIZE_TTL`].
    fn snapshot_bytes(&self) -> u64 {
        let mut cached = lock(&self.snapshot_size);
        if let Some((at, bytes)) = *cached
            && at.elapsed() < SNAPSHOT_SIZE_TTL
        {
            return bytes;
        }
        let dir = storage::snapshot_dir(&self.data_dir);
        let bytes = std::fs::read_dir(dir)
            .map(|rd| {
                rd.filter_map(Result::ok)
                    .filter(|e| e.file_name().to_string_lossy().ends_with(".snap"))
                    .filter_map(|e| e.metadata().ok())
                    .map(|m| m.len())
                    .sum()
            })
            .unwrap_or(0);
        *cached = Some((Instant::now(), bytes));
        bytes
    }

    /// Records that `peer` sent this node a forward, ping or control
    /// request.
    fn heard_from(&self, peer: NodeId) {
        lock(&self.heard).insert(peer, Instant::now());
    }

    fn last_heard(&self, peer: NodeId) -> Option<Instant> {
        lock(&self.heard).get(&peer).copied()
    }

    /// Whether this node belongs to a running cluster: it has applied (or
    /// learned the commit of) an entry beyond the bootstrap membership.
    /// Answered to `--cluster-init` probes ([`handler`]).
    async fn established(&self) -> bool {
        if self.applied_index().is_some_and(|a| a >= 1) {
            return true;
        }
        self.raft
            .with_raft_state(|st| st.committed.map(|c| c.index))
            .await
            .ok()
            .flatten()
            .is_some_and(|c| c >= 1)
    }

    /// Rejoin mode (see the module docs): returns once this node has
    /// applied an entry the leader proposed for it after this process
    /// started, then leaves rejoin mode.
    async fn rejoin(&self) -> Result<(), StartError> {
        tracing::warn!(
            "rejoin mode: this node started without Raft state (or did not finish rejoining); \
             it does not vote or stand for election, and serves no clients, until it has \
             caught up with a leader"
        );
        loop {
            match self.leader() {
                Some(l) if l != self.id => {
                    let op = Op::DropNode {
                        node: self.id,
                        up_to_local: self.state.highest_local(self.id),
                    };
                    match self.control(op).await {
                        ControlOutcome::Applied(i) => {
                            self.applied_up_to(i).await;
                            tracing::info!(index = i, "rejoin: caught up with the leader");
                            break;
                        }
                        // Perhaps proposed (the leader may be busy sending
                        // this node a snapshot): give it time.
                        ControlOutcome::Unknown => {
                            tokio::time::sleep(Duration::from_secs(2)).await;
                        }
                        ControlOutcome::NotProposed => {}
                    }
                }
                _ => {}
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        durable::clear_rejoin(&self.data_dir).map_err(|e| {
            StartError::Other(format!(
                "cannot remove the rejoin marker in {}: {e}",
                self.data_dir.display()
            ))
        })?;
        self.raft.runtime_config().elect(true);
        self.gate.open();
        self.status.rejoining.store(false, Ordering::Release);
        tracing::warn!("rejoin complete: this node votes and stands for election again");
        Ok(())
    }
}

/// `--cluster-init`: asks every other peer whether it already belongs to a
/// running cluster (an empty forward, answered as described in
/// [`handler`]). Returns the first one that does. Peers that cannot be
/// reached, or refuse the connection, are skipped.
async fn established_peer(net: &Network, id: NodeId, peers: &BTreeSet<NodeId>) -> Option<NodeId> {
    let probes = peers.iter().filter(|&&p| p != id).map(|&p| {
        let net = net.clone();
        async move {
            let req = ForwardRequest {
                from: id,
                items: Vec::new(),
            };
            (p, net.forward(p, req).await)
        }
    });
    let mut found = None;
    for (p, r) in futures::future::join_all(probes).await {
        match r {
            Ok(ForwardResponse::Accepted | ForwardResponse::NotLeader { leader: Some(_) }) => {
                found.get_or_insert(p);
            }
            Ok(ForwardResponse::NotLeader { leader: None }) => {
                tracing::info!(peer = p, "--cluster-init: peer has no cluster state yet");
            }
            Err(e) => tracing::info!(peer = p, "--cluster-init: peer not reachable ({e})"),
        }
    }
    found
}

impl ClusterInfo for Core {
    fn ready(&self) -> bool {
        self.status.ready.load(Ordering::Acquire)
    }

    fn stats(&self) -> ClusterStats {
        let m = self.metrics.borrow().clone();
        let log = self.log.metrics();
        let role = match m.state {
            ServerState::Leader => "leader",
            ServerState::Follower => "follower",
            ServerState::Candidate => "candidate",
            ServerState::Learner => "learner",
            ServerState::Shutdown => "shutdown",
        };
        let last_log = m.last_log_index;
        let replication_lag = m.replication.as_ref().map(|r| {
            r.iter()
                .filter(|(id, _)| **id != self.id)
                .map(|(&id, matched)| {
                    let have = matched.map_or(0, |l| l.index + 1);
                    let want = last_log.map_or(0, |l| l + 1);
                    (id, want.saturating_sub(have))
                })
                .collect::<BTreeMap<_, _>>()
        });
        let committed = self.status.committed.load(Ordering::Acquire);
        ClusterStats {
            node_id: self.id,
            role,
            term: m.current_term,
            leader_id: m.current_leader,
            commit_index: (committed != u64::MAX).then_some(committed),
            applied_index: m.last_applied.map(|l| l.index),
            last_log_index: last_log,
            replication_lag,
            log_bytes: log.bytes,
            log_segments: log.segments,
            log_first_index: log.first_index,
            snapshot_index: m.snapshot.map(|l| l.index),
            snapshot_bytes: self.snapshot_bytes(),
            forward_queue: self.status.queue_len.load(Ordering::Relaxed),
            forward_queue_bytes: self.status.queue_bytes.load(Ordering::Relaxed),
            forward_queue_full: self.status.queue_full.load(Ordering::Relaxed),
            refused_connections: self.clients.refused.load(Ordering::Relaxed),
            rejected_puts: self.status.rejected_puts.load(Ordering::Relaxed),
            resent_items: self.status.resent_items.load(Ordering::Relaxed),
            rewinds: [
                ("view", self.status.rewinds_view.load(Ordering::Relaxed)),
                ("error", self.status.rewinds_error.load(Ordering::Relaxed)),
                ("stall", self.status.rewinds_stall.load(Ordering::Relaxed)),
                (
                    "dropped",
                    self.status.rewinds_dropped.load(Ordering::Relaxed),
                ),
            ],
            drop_node_proposals: self.status.drop_node_proposals.load(Ordering::Relaxed),
            ready: self.ready(),
            isolated: self.status.isolated.load(Ordering::Relaxed),
            rejoining: self.status.rejoining.load(Ordering::Relaxed),
            votes_refused: self.gate.refused(),
            next_local_conn: self.conn_ids.get().map(|c| c.peek_local()),
        }
    }
}

/// Why cluster startup failed; `main` maps it to an exit status.
#[derive(Debug)]
pub enum StartError {
    /// Another process holds the data directory (exit status 10).
    Locked(PathBuf),
    /// Anything else (exit status 1).
    Other(String),
}

/// A running cluster node.
pub struct ClusterNode {
    pub core: Arc<Core>,
    pub engine: EngineHandle,
    /// This process's connection ids.
    pub conn_ids: Arc<durable::ConnIdBlocks>,
    listener: Option<ClusterListener>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl ClusterNode {
    pub fn clients(&self) -> Arc<Clients> {
        self.core.clients.clone()
    }

    pub fn info(&self) -> Arc<dyn ClusterInfo> {
        self.core.clone()
    }

    /// After the actor has finished (`EngineMsg::Shutdown`): stops the
    /// background tasks, the cluster listener and Raft.
    pub async fn shutdown(mut self) {
        for t in self.tasks.drain(..) {
            t.abort();
        }
        if let Some(l) = self.listener.take() {
            l.shutdown().await;
        }
        if let Err(e) = self.core.raft.shutdown().await {
            tracing::warn!("raft shutdown: {e}");
        }
    }
}

/// The openraft configuration for `c`.
fn raft_config(c: &ClusterSettings) -> Result<Arc<openraft::Config>, String> {
    let ms = |d: Duration| u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
    let config = openraft::Config {
        cluster_name: "beanstalkd-rs".to_string(),
        heartbeat_interval: ms(c.heartbeat),
        election_timeout_min: ms(c.election_timeout.0),
        election_timeout_max: ms(c.election_timeout.1),
        // One chunk of up to 3 MiB per RPC; the receiver syncs the whole
        // snapshot after the last one.
        install_snapshot_timeout: 5_000,
        snapshot_policy: SnapshotPolicy::LogsSinceLast(c.snapshot_every),
        snapshot_max_chunk_size: SNAPSHOT_CHUNK as u64,
        // Keep at most one snapshot interval of log behind a snapshot.
        max_in_snapshot_log_to_keep: c.snapshot_every.min(1000),
        ..Default::default()
    };
    config
        .validate()
        .map(Arc::new)
        .map_err(|e| format!("cluster timing: {e}"))
}

/// Everything `start` needs besides the settings.
pub struct StartArgs<'a> {
    pub settings: &'a ClusterSettings,
    pub tls: Option<ClusterTls>,
    pub listener: tokio::net::TcpListener,
    pub engine: EngineConfig,
    pub sys: Arc<ProcessSysInfo>,
}

/// How this process starts (see the module docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Bootstrap,
    Rejoin,
    Restart,
}

/// Starts cluster mode and returns once clients may be accepted (see the
/// module docs). Runs until then even without a leader: a node without
/// state waits to be contacted.
pub async fn start(args: StartArgs<'_>) -> Result<ClusterNode, StartError> {
    let c = args.settings;
    let id = c.node_id;
    let clock = Clock::start();
    let other = |what: &str, e: &dyn std::fmt::Display| {
        StartError::Other(format!("{what} {}: {e}", c.data_dir.display()))
    };

    // Before `open`, which creates files of its own.
    let had_state = match storage::has_state(&c.data_dir) {
        Ok(v) => v,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
        Err(e) => return Err(other("cannot inspect cluster.data_dir", &e)),
    };

    let (events_tx, events_rx) = mpsc::unbounded_channel();
    let clients = Clients::new(events_tx);
    let sys = args.sys;
    let sm_opts = SmOptions {
        node_id: id,
        engine: args.engine.clone(),
        sys: Arc::new(move || Box::new(SharedSysInfo(sys.clone()))),
        sink: clients.clone(),
    };
    let (log, sm) = match storage::open(&c.data_dir, LogOptions::default(), sm_opts) {
        Ok(opened) => opened,
        Err(storage::OpenError::Locked(p)) => return Err(StartError::Locked(p)),
        Err(e) => return Err(other("cannot open cluster.data_dir", &e)),
    };
    let state = sm.handle();
    let marked = durable::rejoin_marked(&c.data_dir);
    // Read now, so that a bad file stops the node before it joins.
    let persisted_ids = durable::read_conn_ids(&c.data_dir).map_err(StartError::Other)?;

    let mut net_cfg = NetworkConfig::new(
        id,
        c.peers.clone(),
        args.tls.as_ref().map(|t| t.client.clone()),
    );
    net_cfg.max_job_size = args.engine.max_job_size;
    net_cfg.connect_timeout = Duration::from_secs(1);
    net_cfg.forward_timeout = Duration::from_secs(2);
    net_cfg.backoff_max = Duration::from_millis(500);
    let net = Network::new(net_cfg);

    let config = raft_config(c).map_err(StartError::Other)?;
    let raft = Raft::new(id, config, net.clone(), log.clone(), sm)
        .await
        .map_err(|e| StartError::Other(format!("cannot start raft: {e}")))?;

    let peers: BTreeSet<NodeId> = c.peers.keys().copied().collect();
    let mut mode = match (marked, c.init, had_state) {
        (true, true, _) => {
            return Err(StartError::Other(format!(
                "--cluster-init: {} holds a rejoin marker: this node belongs to a cluster \
                 already (start it without --cluster-init)",
                c.data_dir.display()
            )));
        }
        (true, false, _) | (false, false, false) => Mode::Rejoin,
        (false, true, _) => Mode::Bootstrap,
        (false, false, true) => Mode::Restart,
    };
    if mode == Mode::Bootstrap
        && let Some(peer) = established_peer(&net, id, &peers).await
    {
        tracing::warn!(
            peer,
            "--cluster-init: node {peer} already belongs to a running cluster, so this node \
             must not bootstrap one; joining it in rejoin mode instead"
        );
        mode = Mode::Rejoin;
    }
    let gate = Arc::new(VoteGate::new(mode != Mode::Rejoin));
    if mode == Mode::Rejoin {
        // Before anything can reach Raft through the listener.
        raft.runtime_config().elect(false);
        if !marked {
            durable::mark_rejoin(&c.data_dir)
                .map_err(|e| other("cannot write the rejoin marker in", &e))?;
        }
    }

    let (reaper, reaper_rx) = mpsc::unbounded_channel();
    let reaper_task = tokio::spawn(reap(reaper_rx));
    let core = Arc::new(Core {
        reaper,
        id,
        metrics: raft.metrics(),
        raft: raft.clone(),
        state,
        net,
        log,
        data_dir: c.data_dir.clone(),
        clock,
        stamped: AtomicU64::new(0),
        node_timeout: c.node_timeout,
        peers,
        clients,
        status: Status {
            committed: AtomicU64::new(u64::MAX),
            rejoining: AtomicBool::new(mode == Mode::Rejoin),
            ..Status::default()
        },
        gate: gate.clone(),
        heard: Mutex::new(HashMap::new()),
        conn_ids: OnceLock::new(),
        snapshot_size: Mutex::new(None),
    });

    if mode == Mode::Bootstrap {
        let members: BTreeMap<NodeId, BasicNode> = c
            .peers
            .iter()
            .map(|(&n, addr)| (n, BasicNode::new(addr)))
            .collect();
        raft.initialize(members)
            .await
            .map_err(|e| StartError::Other(format!("--cluster-init: {e}")))?;
        tracing::info!(peers = c.peers.len(), "cluster membership initialized");
    }

    let mut lcfg = ListenerConfig::new(
        id,
        core.peers.clone(),
        args.tls.as_ref().map(|t| t.server.clone()),
    );
    lcfg.max_job_size = args.engine.max_job_size;
    lcfg.vote_gate = Some(gate);
    let listener = ClusterListener::spawn(
        args.listener,
        lcfg,
        raft.clone(),
        Arc::new(handler::Handler::new(core.clone())),
    )
    .map_err(|e| StartError::Other(format!("cluster listener: {e}")))?;
    tracing::info!(node = id, addr = %listener.local_addr(), ?mode, "cluster listener started");

    // The actor and the background duties run from now on (the actor
    // serves nothing until clients connect, but it pings the leader, and
    // the readiness monitor and the leader's duties are needed while this
    // node catches up).
    let (engine_tx, engine_rx) = mpsc::unbounded_channel();
    let mut tasks = vec![
        tokio::spawn(actor::Actor::new(core.clone()).run(engine_rx, events_rx)),
        tokio::spawn(duties::leader_duties(core.clone())),
        tokio::spawn(duties::readiness(core.clone())),
        reaper_task,
    ];
    let abort_all = |tasks: &mut Vec<tokio::task::JoinHandle<()>>| {
        for t in tasks.drain(..) {
            t.abort();
        }
    };

    match mode {
        Mode::Rejoin => {
            if let Err(e) = core.rejoin().await {
                abort_all(&mut tasks);
                return Err(e);
            }
        }
        Mode::Bootstrap | Mode::Restart => tracing::info!("waiting for a leader"),
    }
    core.drop_previous_connections().await;

    // Connection numbers: above everything an earlier process may have
    // handed out (the persisted block end) and everything the replicated
    // state has seen for this node.
    let highest = core.state.highest_local(id);
    let first_local = match persisted_ids {
        Some(p) => p.max(highest.saturating_add(1)),
        None if highest > 0 => highest.saturating_add(CONN_ID_GAP + 1),
        None => highest.saturating_add(1),
    };
    let conn_ids =
        match durable::ConnIdBlocks::open(&c.data_dir, id, first_local, durable::CONN_ID_BLOCK) {
            Ok(ids) => Arc::new(ids),
            Err(e) => {
                abort_all(&mut tasks);
                return Err(StartError::Other(format!("connection numbers: {e}")));
            }
        };
    let _ = core.conn_ids.set(conn_ids.clone());
    core.status.started.store(true, Ordering::Release);
    tracing::info!(node = id, first_local, "cluster node ready");
    Ok(ClusterNode {
        core: core.clone(),
        engine: EngineHandle::Task(engine_tx),
        conn_ids,
        listener: Some(listener),
        tasks: {
            // The actor ends on `EngineMsg::Shutdown`; the others are
            // aborted by `shutdown`.
            let actor = tasks.remove(0);
            drop(actor);
            tasks
        },
    })
}

/// `--cluster-init` refuses a data directory that already holds state, or
/// the rejoin marker (the reachable peers are asked later, in [`start`]).
pub fn check_init(data_dir: &Path) -> Result<(), String> {
    if durable::rejoin_marked(data_dir) {
        return Err(format!(
            "--cluster-init: {} holds a rejoin marker: this node belongs to a cluster already \
             and has not caught up yet (start it without --cluster-init)",
            data_dir.display()
        ));
    }
    match storage::has_state(data_dir) {
        Ok(false) => Ok(()),
        Ok(true) => Err(format!(
            "--cluster-init: {} already holds Raft state; a cluster is bootstrapped only \
             once (start this node without --cluster-init to rejoin its cluster)",
            data_dir.display()
        )),
        Err(e) => Err(format!(
            "--cluster-init: cannot inspect {}: {e}",
            data_dir.display()
        )),
    }
}
