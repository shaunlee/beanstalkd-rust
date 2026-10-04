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
//!                              │  (seq, input)                  │ proposer: Op::Batch
//!                              │                                ▼
//!                              │                        Raft log (majority)
//!                              │                                │ apply, on every node
//!   reply channels ◀──deliver── ReplySink (Clients) ◀──── ClusterStateMachine
//! ```
//!
//! - The actor numbers each connection's inputs (`seq`, from 1 with
//!   `Connect`) and appends them to one ordered queue. A single sender drains
//!   the queue to the current leader: to the [`proposer`] when this node
//!   leads, otherwise as batched `ForwardRequest`s, one in flight at a time.
//!   The leader's proposer turns all the inputs it has queued (its own and
//!   forwarded ones) into `Op::Batch` entries. See [`actor`] for the ordering
//!   and resend rules.
//! - Every node applies every committed entry to its own engine
//!   (`ClusterStateMachine`), and [`Clients`] (the `ReplySink`) hands the
//!   replies for this node's connections to their reply channels.
//! - [`handler::Handler`] serves the cluster port's forwards and control
//!   requests (on the leader it proposes, elsewhere it answers `NotLeader`).
//! - [`duties`] runs the time-driven work: `Tick` proposals and node liveness
//!   (`DropNode`) on the leader, and readiness on every node.
//! - [`membership`] makes the effective Raft membership the authority for
//!   peer addresses and the listener allowlist once the node has one; the
//!   config's `[[cluster.peer]]` list is seeds plus local address overrides.
//! - [`admin`] runs operators' membership changes on the leader (the admin
//!   channel of the cluster port) and finishes a joint configuration left
//!   by an earlier leader.
//!
//! # Startup ([`start`])
//!
//! Open the storage (a locked data directory exits with status 10) and start
//! the cluster listener *before* Raft: until Raft runs it answers only status
//! probes (from the log store, `bstk_raft::status`). Then pick the startup
//! mode (docs/DESIGN.md §8 "Startup modes", "Bootstrap", "Rejoin"):
//!
//! - **Restart** (the data directory holds state, no rejoin marker): start
//!   Raft as it is.
//! - **Bootstrap** (`--cluster-init`, empty data directory, this node in
//!   `cluster.initial_voters`): probe the other initial voters until
//!   `bstk_raft::status::bootstrap_decision` decides to initialize the
//!   membership with the initial voters, or, as soon as one belongs to a
//!   running cluster, to go to discovery (with a warning: a wiped node started
//!   with `--cluster-init` by mistake); otherwise keep asking.
//! - **Discovery** (an empty data directory otherwise, or the marker
//!   [`durable::REJOIN_FILE`] left by an unfinished rejoin; [`discover`]):
//!   learn the current membership from the seeds and its nodes, then join
//!   (wait until added), refuse (a removed or skipped id), or rejoin: the
//!   node may have acknowledged entries and granted votes it no longer
//!   remembers, so it writes the marker, persists the highest vote of
//!   enough current voters (`status::startup_decision`) and only then
//!   starts Raft, with elections disabled and the vote gate closed, serving
//!   no clients. It leaves rejoin mode once it has applied a `DropNode(self)`
//!   entry proposed after this process started (so everything committed
//!   before is in its log); a crash before that keeps the marker.
//!
//! Then wait until a leader is known and this node has applied everything it
//! knows to be committed, close out the connections of this node's previous
//! process with `DropNode(self)` and wait until that is applied here. Only
//! then are clients accepted, with connection numbers from durably reserved
//! blocks ([`durable::ConnIdBlocks`], starting at [`durable::first_local`]).

pub mod actor;
pub mod admin;
pub mod durable;
pub mod duties;
pub mod handler;
pub mod membership;
pub mod proposer;
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub mod test_hooks;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

use openraft::metrics::RaftServerMetrics;
use openraft::storage::RaftLogStorage;
use openraft::{BasicNode, Raft, RaftMetrics, ServerState, SnapshotPolicy, StoredMembership, Vote};
use tokio::sync::{mpsc, oneshot, watch};

use bstk_engine::{ConnId, EngineConfig, Nanos};
use bstk_proto::Response;
use bstk_raft::client::{Network, NetworkConfig};
use bstk_raft::forward::{ControlRequest, ControlResponse, ForwardError};
use bstk_raft::listener::{ClusterListener, ListenerConfig, VoteGate};
use bstk_raft::status::{self, Bootstrap, MembershipView, NodeStatusEx, StatusSource};
use bstk_raft::storage::{self, LogOptions, LogStore, ReplySink, SmOptions, StateHandle};
use bstk_raft::tls::ClusterTls;
use bstk_raft::{CONN_SEQ_BITS, NodeId, Op, Request, TypeConfig, owner_of};

use crate::config::{ClusterSettings, SNAPSHOT_CHUNK};
use crate::engine_actor::{Clock, EngineHandle};
use crate::metrics::{self, ClusterInfo, ClusterStats, MembershipStats};
use crate::sysinfo::{ProcessSysInfo, SharedSysInfo};

/// How long a leader waits for a control proposal to be applied before
/// answering without its index (the requester's timeout is longer).
const CONTROL_APPLY_BOUND: Duration = Duration::from_secs(1);

/// Upper bound on a shutdown's wait for the `Disconnect`s of the closed
/// client connections to be applied.
pub const SHUTDOWN_BOUND: Duration = Duration::from_secs(1);

type Metrics = RaftMetrics<NodeId, BasicNode>;
type ServerMetrics = RaftServerMetrics<NodeId, BasicNode>;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

pub fn local_of(conn: ConnId) -> u64 {
    conn & ((1 << CONN_SEQ_BITS) - 1)
}

#[derive(Debug)]
pub enum Event {
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

    pub fn close(&self, conn: ConnId) {
        if let Some(tx) = lock(&self.closers).remove(&conn) {
            let _ = tx.send(());
        }
    }

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
    /// Shared with the status source ([`StatusView`]), which answers before
    /// `Core` exists.
    rejoining: Arc<AtomicBool>,
    committed: AtomicU64,
    queue_len: AtomicU64,
    queue_bytes: AtomicU64,
    /// The forward queue is at its bound (new clients refused, puts
    /// answered `OUT_OF_MEMORY`).
    queue_full: AtomicBool,
    rejected_puts: AtomicU64,
    /// Items sent again (to the leader, or proposed again as leader) after
    /// having been sent once: duplicates the state machine discards.
    resent_items: AtomicU64,
    rewinds_view: AtomicU64,
    rewinds_error: AtomicU64,
    rewinds_stall: AtomicU64,
    rewinds_dropped: AtomicU64,
    drop_node_proposals: AtomicU64,
}

/// Answers the cluster port's status probes from the log store, and
/// `StatusEx` probes and admin `Membership` requests from everything this
/// node knows: before Raft runs, the effective membership read from storage
/// at startup; once it runs, openraft's metrics.
struct StatusView {
    log: LogStore,
    state: StateHandle,
    rejoining: Arc<AtomicBool>,
    startup_membership: StoredMembership<NodeId, BasicNode>,
    metrics: OnceLock<watch::Receiver<Metrics>>,
}

impl StatusSource for StatusView {
    fn status(&self) -> status::NodeStatus {
        self.log.status()
    }

    fn status_ex(&self) -> NodeStatusEx {
        let durable = self.log.status();
        let mut ex = NodeStatusEx::from_status(durable);
        ex.rejoining = self.rejoining.load(Ordering::Relaxed);
        ex.highest_member = self.state.highest_member();
        let membership = match self.metrics.get() {
            Some(rx) => {
                let m = rx.borrow();
                ex.raft_running = m.running_state.is_ok();
                ex.term = m.current_term;
                ex.leader = m.current_leader;
                ex.last_applied = m.last_applied;
                (*m.membership_config).clone()
            }
            None => {
                ex.last_applied = self.state.last_applied();
                self.startup_membership.clone()
            }
        };
        // By index: both come from this node's log, whose committed prefix
        // never diverges from the leader's.
        let known = [durable.committed, ex.last_applied]
            .into_iter()
            .flatten()
            .map(|l| l.index)
            .max();
        let committed = membership
            .log_id()
            .is_some_and(|l| known.is_some_and(|k| k >= l.index));
        ex.membership = MembershipView::from_stored(&membership, committed);
        // Before Raft runs only a snapshot is applied, so the effective
        // membership may name higher ids than the applied record.
        ex.highest_member = ex
            .membership
            .nodes
            .keys()
            .copied()
            .fold(ex.highest_member, NodeId::max);
        ex
    }
}

pub struct Core {
    id: NodeId,
    raft: Raft<TypeConfig>,
    metrics: watch::Receiver<Metrics>,
    server: watch::Receiver<ServerMetrics>,
    state: StateHandle,
    net: Network,
    log: LogStore,
    data_dir: PathBuf,
    clock: Clock,
    stamped: AtomicU64,
    node_timeout: Duration,
    clients: Arc<Clients>,
    status: Status,
    /// Keeps `client_write_ff` receivers until they resolve (openraft
    /// logs a warning for every result it cannot deliver).
    reaper: mpsc::UnboundedSender<Pending>,
    /// Connection inputs to propose as `Op::Batch` entries (leader only,
    /// see [`proposer`]).
    proposer: mpsc::UnboundedSender<proposer::Items>,
    gate: Arc<VoteGate>,
    /// When each peer last sent this node a forward, ping or control
    /// request (leader-side liveness, [`duties`]).
    heard: Mutex<HashMap<NodeId, Instant>>,
    conn_ids: OnceLock<Arc<durable::ConnIdBlocks>>,
    snapshot_size: Mutex<Option<(Instant, u64)>>,
    /// Held by the membership change in progress ([`admin`]): one at a
    /// time, including the finishing of a leftover joint configuration.
    admin_lock: Arc<tokio::sync::Mutex<()>>,
    /// The nodes the listener admits (see [`Core::is_member`]).
    admitted: Mutex<BTreeSet<NodeId>>,
    /// Cluster traffic uses mTLS (the admin executor's address checks).
    tls: bool,
    plaintext_allow_remote: bool,
    heartbeat: Duration,
}

/// How often `/metrics` rescans the snapshot directory at most.
const SNAPSHOT_SIZE_TTL: Duration = Duration::from_secs(3);

/// A proposal's result receiver, awaited by [`reap`] and discarded.
type Pending = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

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

#[derive(Debug)]
struct RaftStopped;

#[derive(Debug)]
enum ControlOutcome {
    Applied(u64),
    Unknown,
    NotProposed,
}

impl Core {
    fn leader(&self) -> Option<NodeId> {
        self.server.borrow().current_leader
    }

    fn is_leader(&self) -> bool {
        let m = self.server.borrow();
        m.current_leader == Some(self.id) && m.state == ServerState::Leader
    }

    fn view(&self) -> (Option<NodeId>, u64) {
        let m = self.server.borrow();
        (m.current_leader, m.vote.leader_id().get_term())
    }

    /// A receiver that wakes only when the leader, vote, role or
    /// membership changes. The leader / role / term readers above use the
    /// same source: openraft publishes it before the full metrics, so a
    /// task woken here could still see stale full metrics
    /// (docs/DESIGN.md §8, "Fewer wake-ups").
    fn watch_view(&self) -> watch::Receiver<ServerMetrics> {
        self.raft.server_metrics()
    }

    /// The `now` of a new proposal: never below this node's clock, the
    /// last applied `now`, or any `now` this node has already proposed.
    fn stamp(&self) -> Nanos {
        let now = self.clock.now().max(self.state.last_now());
        let prev = self.stamped.fetch_max(now, Ordering::AcqRel);
        prev.max(now)
    }

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

    /// Proposes `op` like [`Core::propose`]; the returned future resolves
    /// once the result is known (applied here, or refused).
    async fn propose_tracked(
        &self,
        op: Op,
    ) -> Result<impl std::future::Future<Output = ()> + Send + 'static, RaftStopped> {
        let req = Request {
            now: self.stamp(),
            op,
        };
        match self.raft.client_write_ff(req).await {
            Ok(rx) => Ok(async move {
                let _ = rx.await;
            }),
            Err(e) => {
                tracing::debug!("proposal refused: {e}");
                Err(RaftStopped)
            }
        }
    }

    /// Hands connection inputs `(seq, input)` to the proposer, which
    /// proposes them in order as `Op::Batch` entries. `Err` means the
    /// proposer has stopped (Raft has stopped).
    fn submit(&self, items: proposer::Items) -> Result<(), RaftStopped> {
        if items.is_empty() {
            return Ok(());
        }
        self.proposer.send(items).map_err(|_| RaftStopped)
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

    /// The effective membership (the latest in this node's log).
    fn membership(&self) -> Arc<membership::Membership> {
        self.server.borrow().membership_config.clone()
    }

    /// Whether `node` may send forwards and control requests: a node the
    /// listener admits ([`membership::allowed`]: the effective and the
    /// committed membership), or of the effective membership before
    /// [`membership::watch`] first ran.
    fn is_member(&self, node: NodeId) -> bool {
        let admitted = lock(&self.admitted);
        if admitted.is_empty() {
            return membership::is_member(&self.server.borrow().membership_config, node);
        }
        admitted.contains(&node)
    }

    fn set_admitted(&self, nodes: BTreeSet<NodeId>) {
        *lock(&self.admitted) = nodes;
    }

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
        leader_reachable(
            self.id,
            m.current_leader,
            membership::voter_count(&m.membership_config),
            m.millis_since_quorum_ack,
            self.node_timeout,
        )
    }

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

    fn heard_from(&self, peer: NodeId) {
        lock(&self.heard).insert(peer, Instant::now());
    }

    fn last_heard(&self, peer: NodeId) -> Option<Instant> {
        lock(&self.heard).get(&peer).copied()
    }

    /// Rejoin mode (see the module docs): returns once this node has
    /// applied an entry the leader proposed for it after this process
    /// started, then leaves rejoin mode. Every [`REJOIN_RECHECK`] until then
    /// it asks `targets` (the seeds and the nodes of the membership
    /// discovery decided on) and this node's membership's nodes for their
    /// status, and gives up if [`status::startup_decision`] refuses this id:
    /// it was removed meanwhile, and no leader would ever propose its
    /// `DropNode`.
    async fn rejoin(&self, targets: &BTreeSet<NodeId>) -> Result<(), StartError> {
        let mut checked = Instant::now();
        loop {
            #[cfg(feature = "test-hooks")]
            let held = test_hooks::rejoin_held(&self.data_dir);
            #[cfg(not(feature = "test-hooks"))]
            let held = false;
            match self.leader() {
                Some(l) if l != self.id && !held => {
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
            if checked.elapsed() >= REJOIN_RECHECK {
                checked = Instant::now();
                if let Some(reason) = self.removed_while_rejoining(targets).await {
                    return Err(StartError::Other(format!(
                        "cannot finish rejoining node {}: {reason}",
                        self.id
                    )));
                }
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

impl Core {
    /// `Some(reason)` if the other nodes' status says this id is no longer
    /// a member and never will be again (see [`Core::rejoin`]).
    async fn removed_while_rejoining(&self, targets: &BTreeSet<NodeId>) -> Option<String> {
        let mut ask = targets.clone();
        ask.extend(self.membership().membership().nodes().map(|(&n, _)| n));
        let (answers, _) = status::probe_ex(&self.net, self.id, &ask).await;
        match status::startup_decision(self.id, &answers, None) {
            status::Startup::Refuse(reason) => Some(reason),
            _ => None,
        }
    }
}

/// How often a node in rejoin mode checks that it is still a member.
const REJOIN_RECHECK: Duration = Duration::from_secs(3);

/// See [`Core::leader_reachable`]. A single voter needs no acknowledgement
/// (openraft refreshes `millis_since_quorum_ack` only when its core loop
/// runs); the count is the effective membership's, not the config's.
fn leader_reachable(
    id: NodeId,
    leader: Option<NodeId>,
    voters: usize,
    millis_since_quorum_ack: Option<u64>,
    node_timeout: Duration,
) -> bool {
    match leader {
        None => false,
        Some(l) if l == id => {
            voters == 1
                || millis_since_quorum_ack
                    .is_some_and(|ms| u128::from(ms) <= node_timeout.as_millis())
        }
        Some(_) => true,
    }
}

/// How long a startup probe loop waits between rounds at most, and how
/// often it logs that it is still waiting.
const PROBE_BACKOFF_MAX: Duration = Duration::from_secs(2);
const PROBE_LOG_EVERY: Duration = Duration::from_secs(5);

/// Discovery, for a node without usable Raft state (an empty data
/// directory, or the rejoin marker): asks the config seeds, and then the
/// nodes of the membership they report, for their [`NodeStatusEx`] (over
/// probe connections, so a node that is not a member yet is answered) until
/// [`status::startup_decision`] decides. Returns the vote to start Raft with
/// once this node is a member (rejoin; a node that joins waits here until
/// the operator has added it); a removed or skipped id, or a voter that no
/// quorum of other voters can vouch for, is an error. While waiting, the
/// learned membership's nodes are allowed to connect and are dialed at its
/// addresses (config overrides first), so the leader that adds this node
/// need not be a seed. `local` is this node's own persisted vote (a restart
/// with the rejoin marker). Also returns the nodes to ask while rejoining
/// (the seeds and the nodes of the membership decided on, see
/// [`Core::rejoin`]). Safety: docs/DESIGN.md §8 "Why rejoin is safe".
#[allow(clippy::too_many_arguments)]
async fn discover(
    net: &Network,
    allowlist: &bstk_raft::listener::PeerAllowlist,
    id: NodeId,
    overrides: &BTreeMap<NodeId, String>,
    mut local: Option<Vote<NodeId>>,
    log: &LogStore,
    data_dir: &Path,
    joining: &AtomicBool,
) -> Result<(Option<Vote<NodeId>>, BTreeSet<NodeId>), StartError> {
    let mut marked = durable::rejoin_marked(data_dir);
    let save = async |v: &Vote<NodeId>| {
        let mut store = log.clone();
        RaftLogStorage::save_vote(&mut store, v)
            .await
            .map_err(|e| StartError::Other(format!("rejoin: cannot save the vote: {e}")))
    };
    let seeds: BTreeSet<NodeId> = overrides.keys().copied().filter(|&n| n != id).collect();
    if seeds.is_empty() {
        return Err(StartError::Other(
            "rejoin: a single-node cluster cannot rejoin, and a joining node needs another \
             node to learn the cluster from: [[cluster.peer]] lists no other node; restore \
             the data directory, list a node of the running cluster, or start it with \
             --cluster-init to create a new cluster"
                .into(),
        ));
    }
    let mut targets = seeds.clone();
    let mut answers: BTreeMap<NodeId, NodeStatusEx> = BTreeMap::new();
    let mut learned: Option<openraft::LogId<NodeId>> = None;
    let mut delay = Duration::from_millis(50);
    let mut logged: Option<Instant> = None;
    loop {
        let (got, failed) = status::probe_ex(net, id, &targets).await;
        answers.extend(got);
        if let Some(m) = status::learned_membership(&answers)
            && m.log_id != learned
        {
            learned = m.log_id;
            let mut allow: BTreeSet<NodeId> = seeds.clone();
            allow.extend(m.nodes.keys().copied());
            allowlist.set(allow);
            net.set_members(m.nodes.clone());
            targets = seeds.iter().chain(m.nodes.keys()).copied().collect();
            tracing::info!(
                membership = ?m.log_id,
                voters = ?m.configs,
                nodes = ?m.nodes.keys().collect::<Vec<_>>(),
                "startup: learned the cluster membership"
            );
            for d in membership::config_differences(overrides, &m.nodes) {
                tracing::warn!(membership = ?m.log_id, "cluster config differs from the membership: {d}");
            }
        }
        let decision = status::startup_decision(id, &answers, local);
        joining.store(
            matches!(decision, status::Startup::Join { .. }),
            Ordering::Relaxed,
        );
        let why = match decision {
            status::Startup::Rejoin { vote, membership } => {
                // Durable before a vote is saved or Raft starts: a vote file
                // without the marker would make a restart look like an
                // ordinary one, and a crash before the node has caught up
                // must bring it back here with its vote gate closed. (Not
                // earlier: a node waiting to join may be restarted with
                // --cluster-init, which refuses a marker.)
                if !marked {
                    durable::mark_rejoin(data_dir).map_err(|e| {
                        StartError::Other(format!(
                            "cannot write the rejoin marker in {}: {e}",
                            data_dir.display()
                        ))
                    })?;
                    marked = true;
                }
                if let Some(v) = &vote {
                    save(v).await?;
                }
                // Re-check after the vote is durable: a membership committed
                // since the answers above (which P6-T4's guardrails refuse
                // while this node reports `rejoining`) means deciding again,
                // with the saved vote as the floor (votes only increase).
                let voters = membership.voters();
                let (again, _) = status::probe_ex(net, id, &voters).await;
                match status::startup_decision(id, &again, vote) {
                    status::Startup::Rejoin {
                        vote: v2,
                        membership: m2,
                    } if m2.log_id == membership.log_id => {
                        if let Some(v) = v2.filter(|v2| Some(*v2) != vote) {
                            save(&v).await?;
                        }
                        let vote = v2.or(vote);
                        tracing::warn!(
                            membership = ?membership.log_id,
                            voters = ?voters,
                            vote = %vote.map_or_else(|| "none".to_string(), |v| v.to_string()),
                            "rejoin: adopted the highest vote of the current voters that answered"
                        );
                        let mut ask = seeds;
                        ask.extend(membership.nodes.keys().copied());
                        return Ok((vote, ask));
                    }
                    other => {
                        local = vote.or(local);
                        answers = again;
                        format!(
                            "the membership changed or the voters stopped answering while the \
                             vote was adopted ({other:?}); deciding again"
                        )
                    }
                }
            }
            status::Startup::Refuse(reason) => {
                return Err(StartError::Other(format!(
                    "cannot start node {id}: {reason}"
                )));
            }
            status::Startup::Join {
                voters,
                highest_member,
            } => format!(
                "join: node {id} is not a member of the cluster yet (voters {voters:?}, highest \
                 member id {highest_member}); waiting until it is added as a learner"
            ),
            status::Startup::Wait { reason, fresh } => {
                if fresh {
                    answers.clear();
                }
                reason
            }
        };
        if logged.is_none_or(|t| t.elapsed() >= PROBE_LOG_EVERY) {
            tracing::warn!(
                answered = ?answers.keys().collect::<Vec<_>>(),
                unreachable = ?failed,
                "startup: waiting for the cluster's status ({why})"
            );
            logged = Some(Instant::now());
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(PROBE_BACKOFF_MAX);
    }
}

/// `--cluster-init` on an empty data directory, for a node of the initial
/// voter set `voters`: asks the other initial voters for their status until
/// [`status::bootstrap_decision`] decides (each round on that round's answers
/// only). `None`: initialize; `Some(peer)`: `peer` belongs to a running
/// cluster, rejoin it.
async fn probe_bootstrap(net: &Network, id: NodeId, voters: &BTreeSet<NodeId>) -> Option<NodeId> {
    let n = voters.len();
    let mut delay = Duration::from_millis(50);
    let mut logged: Option<Instant> = None;
    loop {
        let answers = status::probe(net, id, voters).await;
        match status::bootstrap_decision(n, &answers) {
            Bootstrap::Wait => {
                if logged.is_none_or(|t| t.elapsed() >= PROBE_LOG_EVERY) {
                    tracing::warn!(
                        answered = answers.len(),
                        of = n - 1,
                        "--cluster-init: waiting for the other nodes' status before \
                         initializing (a majority of them without any state, or all of them)"
                    );
                    logged = Some(Instant::now());
                }
            }
            Bootstrap::Initialize => return None,
            Bootstrap::Rejoin(peer) => return Some(peer),
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(PROBE_BACKOFF_MAX);
    }
}

fn announce_rejoin() {
    tracing::warn!(
        "rejoin mode: this node started without Raft state (or did not finish rejoining); \
         it learns the current membership from the other nodes, joins (waiting until it is \
         added) or adopts the highest vote of enough of the current voters before it starts \
         Raft, and does not vote, stand for election or serve clients until it has caught up \
         with a leader"
    );
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
        let commit_index = (committed != u64::MAX).then_some(committed);
        let applied_index = m.last_applied.map(|l| l.index);
        let (membership, is_member) = membership_stats(
            self.id,
            &m.membership_config,
            commit_index.max(applied_index),
            self.state.highest_member(),
        );
        let learner_lag = replication_lag.as_ref().map(|lag| {
            lag.iter()
                .filter(|(id, _)| membership.learners.contains(id))
                .map(|(&id, &n)| (id, n))
                .collect()
        });
        let rejoining = self.status.rejoining.load(Ordering::Relaxed);
        let started = self.status.started.load(Ordering::Acquire);
        ClusterStats {
            node_id: self.id,
            role,
            term: m.current_term,
            leader_id: m.current_leader,
            commit_index,
            applied_index,
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
            rejoining,
            votes_refused: self.gate.refused(),
            next_local_conn: self.conn_ids.get().map(|c| c.peek_local()),
            phase: phase(false, rejoining, started),
            joining: false,
            membership,
            is_member,
            learner_lag,
        }
    }
}

/// [`ClusterStats::phase`]. A node that joins is also in rejoin mode (it
/// catches up without voting once added), so `joining` comes first.
fn phase(joining: bool, rejoining: bool, started: bool) -> &'static str {
    if joining {
        "joining"
    } else if rejoining {
        "rejoining"
    } else if started {
        "normal"
    } else {
        "starting"
    }
}

/// The membership figures of `m` for node `id`, and whether `id` is a
/// member. `known_committed` is the highest index this node knows to be
/// committed (its commit index or applied index); `highest_applied` is the
/// applied `SmMeta::highest_member`, raised to the ids `m` names (an entry
/// not applied yet may name higher ones).
fn membership_stats(
    id: NodeId,
    m: &StoredMembership<NodeId, BasicNode>,
    known_committed: Option<u64>,
    highest_applied: NodeId,
) -> (MembershipStats, bool) {
    let committed = m
        .log_id()
        .is_some_and(|l| known_committed.is_some_and(|k| k >= l.index));
    view_stats(
        id,
        &MembershipView::from_stored(m, committed),
        highest_applied,
    )
}

fn view_stats(
    id: NodeId,
    view: &MembershipView,
    highest_applied: NodeId,
) -> (MembershipStats, bool) {
    let highest_member = view
        .nodes
        .keys()
        .copied()
        .fold(highest_applied, NodeId::max);
    let stats = MembershipStats {
        voters: view.voters(),
        learners: view.learners(),
        joint: view.is_joint(),
        log_index: view.log_id.map(|l| l.index),
        committed: view.committed,
        highest_member,
        addrs: view.nodes.clone(),
    };
    (stats, view.is_member(id))
}

/// What `/metrics`, `/admin` and `/readyz` see of a cluster node from the
/// moment its cluster listener runs, so that a node still discovering its
/// cluster (joining, rejoining) can be watched: its storage and the status
/// view until [`Core`] exists, then `Core`.
struct ClusterView {
    id: NodeId,
    status: Arc<StatusView>,
    joining: Arc<AtomicBool>,
    core: OnceLock<Arc<Core>>,
}

impl ClusterInfo for ClusterView {
    fn ready(&self) -> bool {
        self.core.get().is_some_and(|c| c.ready())
    }

    fn stats(&self) -> ClusterStats {
        if let Some(core) = self.core.get() {
            return core.stats();
        }
        let ex = self.status.status_ex();
        let log = self.status.log.metrics();
        let joining = self.joining.load(Ordering::Relaxed);
        let (membership, is_member) = view_stats(self.id, &ex.membership, ex.highest_member);
        ClusterStats {
            node_id: self.id,
            role: if ex.raft_running {
                "learner"
            } else {
                metrics::ROLE_STARTING
            },
            term: ex.term,
            leader_id: ex.leader,
            commit_index: ex.status.committed.map(|l| l.index),
            applied_index: ex.last_applied.map(|l| l.index),
            last_log_index: ex.status.last_log_id.map(|l| l.index),
            log_bytes: log.bytes,
            log_segments: log.segments,
            log_first_index: log.first_index,
            rejoining: ex.rejoining,
            phase: phase(joining, ex.rejoining, false),
            joining,
            membership,
            is_member,
            rewinds: metrics::NO_REWINDS,
            ..ClusterStats::default()
        }
    }
}

#[derive(Debug)]
pub enum StartError {
    Locked(PathBuf),
    Other(String),
}

pub struct ClusterNode {
    pub core: Arc<Core>,
    pub engine: EngineHandle,
    pub conn_ids: Arc<durable::ConnIdBlocks>,
    listener: Option<ClusterListener>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl ClusterNode {
    pub fn clients(&self) -> Arc<Clients> {
        self.core.clients.clone()
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

fn raft_config(c: &ClusterSettings, elect: bool) -> Result<Arc<openraft::Config>, String> {
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
        enable_elect: elect,
        ..Default::default()
    };
    config
        .validate()
        .map(Arc::new)
        .map_err(|e| format!("cluster timing: {e}"))
}

pub struct StartArgs<'a> {
    pub settings: &'a ClusterSettings,
    pub tls: Option<ClusterTls>,
    pub listener: tokio::net::TcpListener,
    pub engine: EngineConfig,
    pub sys: Arc<ProcessSysInfo>,
    /// Called once, as soon as the cluster listener runs, with what the
    /// HTTP endpoints show of this node from then on.
    pub publish: Box<dyn FnOnce(Arc<dyn ClusterInfo>) + Send + 'a>,
}

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
    let process_started = Instant::now();
    let c = args.settings;
    let id = c.node_id;
    let clock = Clock::start();
    let other = |what: &str, e: &dyn std::fmt::Display| {
        StartError::Other(format!("{what} {}: {e}", c.data_dir.display()))
    };

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
    let (log, mut sm) = match storage::open(&c.data_dir, LogOptions::default(), sm_opts) {
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

    // The config seeds: whom discovery asks first, and whom the listener
    // accepts until this node has a membership.
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
        // A node outside the initial voters must never initialize (openraft
        // refuses a membership without this node, and the voters would
        // differ from the other initial nodes'): it joins.
        (false, true, _) if !c.initial_voters.contains(&id) => {
            tracing::info!(
                initial_voters = ?c.initial_voters,
                "--cluster-init: this node is not an initial voter; it joins the cluster once \
                 the initial voters have created it and the operator has added it"
            );
            Mode::Rejoin
        }
        (false, true, _) => Mode::Bootstrap,
        (false, false, true) => Mode::Restart,
    };

    // The listener runs from now on; until Raft is installed it answers
    // status probes only. Its vote gate stays closed unless the mode says
    // otherwise.
    let gate = Arc::new(VoteGate::new(false));
    // Until Raft runs (status probes only), the seeds plus the snapshot's
    // membership; `membership::watch` switches to the effective membership
    // from the log once Raft runs.
    let mut initial: BTreeSet<NodeId> = peers.clone();
    if let Some(m) = state.membership() {
        initial.extend(membership::addresses(&m).into_keys());
    }
    // What a StatusEx probe reports before Raft runs: the latest
    // membership in storage, as Raft will load it (nothing appends a
    // membership entry before Raft runs).
    let startup_membership = {
        let mut l = log.clone();
        let m = openraft::storage::StorageHelper::new(&mut l, &mut sm)
            .get_membership()
            .await
            .map_err(|e| other("cannot read the membership from cluster.data_dir", &e))?;
        let e = m.effective();
        StoredMembership::new(*e.log_id(), e.membership().clone())
    };
    let rejoining = Arc::new(AtomicBool::new(mode == Mode::Rejoin));
    let status_view = Arc::new(StatusView {
        log: log.clone(),
        state: state.clone(),
        rejoining: rejoining.clone(),
        startup_membership,
        metrics: OnceLock::new(),
    });
    let mut lcfg = ListenerConfig::new(id, initial, args.tls.as_ref().map(|t| t.server.clone()));
    lcfg.max_job_size = args.engine.max_job_size;
    lcfg.vote_gate = Some(gate.clone());
    lcfg.status = Some(status_view.clone());
    lcfg.plaintext_remote_probes = c.plaintext_allow_remote;
    let (listener, service) =
        ClusterListener::spawn_deferred::<handler::Handler>(args.listener, lcfg)
            .map_err(|e| StartError::Other(format!("cluster listener: {e}")))?;
    tracing::info!(node = id, addr = %listener.local_addr(), ?mode, "cluster listener started");
    let joining = Arc::new(AtomicBool::new(false));
    let view = Arc::new(ClusterView {
        id,
        status: status_view.clone(),
        joining: joining.clone(),
        core: OnceLock::new(),
    });
    (args.publish)(view.clone());

    if mode == Mode::Rejoin {
        announce_rejoin();
    }
    if mode == Mode::Bootstrap
        && let Some(peer) = probe_bootstrap(&net, id, &c.initial_voters).await
    {
        tracing::warn!(
            peer,
            "--cluster-init: node {peer} already belongs to a running cluster, so this node \
             must not bootstrap one; joining it in rejoin mode instead"
        );
        mode = Mode::Rejoin;
        rejoining.store(true, Ordering::Release);
        announce_rejoin();
    }
    let mut rejoin_targets = BTreeSet::new();
    if mode == Mode::Rejoin {
        let local = log.status().vote;
        (_, rejoin_targets) = discover(
            &net,
            &listener.allowlist(),
            id,
            &c.peers,
            local,
            &log,
            &c.data_dir,
            &joining,
        )
        .await?;
    }

    let config = raft_config(c, mode != Mode::Rejoin).map_err(StartError::Other)?;
    let raft = Raft::new(id, config, net.clone(), log.clone(), sm)
        .await
        .map_err(|e| StartError::Other(format!("cannot start raft: {e}")))?;
    let _ = status_view.metrics.set(raft.metrics());

    let (reaper, reaper_rx) = mpsc::unbounded_channel();
    let reaper_task = tokio::spawn(reap(reaper_rx));
    let (proposer_tx, proposer_rx) = mpsc::unbounded_channel();
    let core = Arc::new(Core {
        reaper,
        proposer: proposer_tx,
        id,
        metrics: raft.metrics(),
        server: raft.server_metrics(),
        raft: raft.clone(),
        state,
        net,
        log,
        data_dir: c.data_dir.clone(),
        clock,
        stamped: AtomicU64::new(0),
        node_timeout: c.node_timeout,
        clients,
        status: Status {
            committed: AtomicU64::new(u64::MAX),
            rejoining: rejoining.clone(),
            ..Status::default()
        },
        gate: gate.clone(),
        heard: Mutex::new(HashMap::new()),
        conn_ids: OnceLock::new(),
        snapshot_size: Mutex::new(None),
        admin_lock: Arc::new(tokio::sync::Mutex::new(())),
        admitted: Mutex::new(BTreeSet::new()),
        tls: args.tls.is_some(),
        plaintext_allow_remote: c.plaintext_allow_remote,
        heartbeat: c.heartbeat,
    });
    let _ = view.core.set(core.clone());

    if mode == Mode::Bootstrap {
        let members: BTreeMap<NodeId, BasicNode> = c
            .peers
            .iter()
            .filter(|(n, _)| c.initial_voters.contains(n))
            .map(|(&n, addr)| (n, BasicNode::new(addr)))
            .collect();
        raft.initialize(members)
            .await
            .map_err(|e| StartError::Other(format!("--cluster-init: {e}")))?;
        tracing::info!(voters = ?c.initial_voters, "cluster membership initialized");
    }

    if mode != Mode::Rejoin {
        gate.open();
    }
    service.set(raft.clone(), Arc::new(handler::Handler::new(core.clone())));

    // The actor and the background duties run from now on (the actor
    // serves nothing until clients connect, but it pings the leader, and
    // the readiness monitor and the leader's duties are needed while this
    // node catches up).
    let (engine_tx, engine_rx) = mpsc::unbounded_channel();
    let actor = tokio::spawn(actor::Actor::new(core.clone()).run(engine_rx, events_rx));
    let mut tasks = vec![
        actor,
        tokio::spawn(proposer::run(core.clone(), proposer_rx)),
        tokio::spawn(duties::leader_duties(core.clone())),
        tokio::spawn(duties::readiness(core.clone())),
        reaper_task,
        tokio::spawn(admin::finish_joint(core.clone())),
        tokio::spawn(membership::watch(
            core.clone(),
            listener.allowlist(),
            peers.clone(),
            c.peers.clone(),
        )),
    ];
    #[cfg(feature = "test-hooks")]
    tasks.push(tokio::spawn(test_hooks::run(
        core.clone(),
        c.data_dir.clone(),
    )));
    let abort_all = |tasks: &mut Vec<tokio::task::JoinHandle<()>>| {
        for t in tasks.drain(..) {
            t.abort();
        }
    };

    match mode {
        Mode::Rejoin => {
            if let Err(e) = core.rejoin(&rejoin_targets).await {
                abort_all(&mut tasks);
                return Err(e);
            }
        }
        Mode::Bootstrap => tracing::info!("waiting for a leader"),
        Mode::Restart => {
            // A warning, not an exit: the entry may be an uncommitted one
            // that a new leader truncates.
            let m = &status_view.startup_membership;
            if membership::has_members(m) && !membership::is_member(m, id) {
                tracing::warn!(
                    membership = ?m.log_id(),
                    "this node is not in the membership of its own log: it was removed (or its \
                     removal was not committed yet); a removed node never serves clients again \
                     and its id is never reused: stop it"
                );
            }
            tracing::info!("waiting for a leader");
        }
    }
    core.drop_previous_connections().await;

    // Connection numbers: above everything an earlier process may have
    // handed out (the persisted block end, the replicated state, and the
    // time floor for a wiped node's lost block; see `durable::first_local`).
    let highest = core.state.highest_local(id);
    if persisted_ids.is_none() {
        // The floor is taken at least a second after this process started,
        // so its second is past every second the lost process could have
        // handed numbers in (see `durable::first_local`).
        tokio::time::sleep(durable::FLOOR_DELAY.saturating_sub(process_started.elapsed())).await;
    }
    let first_local = match durable::first_local(persisted_ids, highest, durable::unix_seconds()) {
        Ok(f) => f,
        Err(e) => {
            abort_all(&mut tasks);
            return Err(StartError::Other(format!("connection numbers: {e}")));
        }
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
            // The actor ends by itself on `EngineMsg::Shutdown`; `shutdown`
            // aborts only the others.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leader_reachable_uses_the_voter_count() {
        let t = Duration::from_secs(1);
        // A follower: reachable while it names a leader.
        assert!(leader_reachable(2, Some(1), 3, None, t));
        assert!(!leader_reachable(2, None, 3, None, t));
        // A single voter needs no acknowledgement, whatever the config says.
        assert!(leader_reachable(1, Some(1), 1, None, t));
        // Several voters (a cluster grown from one node): only with a
        // recent quorum acknowledgement.
        assert!(!leader_reachable(1, Some(1), 2, None, t));
        assert!(!leader_reachable(1, Some(1), 2, Some(1001), t));
        assert!(leader_reachable(1, Some(1), 2, Some(1000), t));
    }
}
