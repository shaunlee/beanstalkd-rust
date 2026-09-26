//! The dialing side of the cluster port: openraft's `RaftNetworkFactory` /
//! `RaftNetwork` over TCP (optionally mutual TLS) and the forward client.
//!
//! One [`Network`] is shared (it is `Clone`) by openraft and by the owner's
//! forwarding code. It keeps at most one connection per target node,
//! dialed lazily on the first request. The connection is multiplexed:
//! requests carry ids, so several callers (the replication stream, an
//! election, forwarding) may have requests in flight at once; the listener
//! answers them in order.
//!
//! Failures:
//! - dial failures (TCP, TLS, hello) are `Unreachable`, and further dials
//!   to that target are refused (also `Unreachable`) until a backoff delay
//!   has passed; the delay doubles per consecutive failure, from
//!   `backoff_min` up to `backoff_max`. openraft backs off on `Unreachable`
//!   using the same schedule ([`RaftNetwork::backoff`]).
//! - a request without an answer within its timeout is `Timeout`. Only
//!   that request fails: its pending slot is dropped (a late answer is
//!   discarded by id) and the connection stays up for everyone else. The
//!   connection is closed, and the next request re-dials, only when it is
//!   stalled: a request goes unanswered (times out or is cancelled) while
//!   requests have been outstanding with no response at all for longer
//!   than `max(stall_timeout, 3 × that request's timeout)`. A peer that
//!   stops answering on a live socket is thus detected, but one slow
//!   request (a large AppendEntries, a forward waiting behind it) does not
//!   fail the other users of the link.
//! - a connection lost while a request is in flight is a `Network` error.
//! - an AppendEntries batch of more than one entry whose frame exceeds
//!   `append_budget` bytes (or `max_frame`) is `PayloadTooLarge` with an
//!   entries hint scaled to fit the budget, so openraft splits it; a single
//!   entry is always sent (up to `max_frame`).
//!
//! # Interaction with openraft's timeouts
//!
//! openraft 0.9 wraps every AppendEntries call in its own timeout of
//! `heartbeat_interval` (and passes the same value as the RPC option's
//! hard TTL), and drops the call future when it expires; vote requests
//! are bounded the same way by the election timeout. A dropped call is
//! accounted like a timed-out one (the pending slot is removed by a drop
//! guard and counts towards stall detection), so a replication stream that
//! keeps timing out on a large batch neither leaks slots nor tears down the
//! connection that forwards and votes share with it. The byte budget keeps
//! a batch small enough to cross the link within a heartbeat interval in
//! the common case; a single entry near `-z` must still cross within one
//! heartbeat interval, or its replication keeps timing out (the heartbeat
//! must be set with the largest job body and the link speed in mind).

use std::collections::{BTreeMap, HashMap};

use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use openraft::BasicNode;
use openraft::error::{
    InstallSnapshotError, NetworkError, PayloadTooLarge, RPCError, RaftError, RemoteError, Timeout,
    Unreachable,
};
use openraft::network::{Backoff, RPCOption, RPCTypes, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use rustls::ClientConfig;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, Notify, mpsc, oneshot};
use tokio::time::Instant;
use tokio_rustls::TlsConnector;

use crate::forward::{ControlRequest, ControlResponse, ForwardError, ForwardTransport};
use crate::status::{NodeStatus, StatusTransport};
use crate::wire::{
    self, ClientMsg, FrameError, Hello, PROTOCOL_VERSION, RpcRequest, RpcResponse, ServerHello,
    ServerMsg, WireError, WireFatal,
};
use crate::{ForwardRequest, ForwardResponse, NodeId, TypeConfig};

/// Default [`NetworkConfig::append_budget`]: 1 MiB, about 10 ms on a
/// 1 Gbit/s link, well inside the default 50 ms heartbeat interval.
pub const DEFAULT_APPEND_BUDGET: usize = 1 << 20;

/// Settings of the dialing side.
#[derive(Clone)]
pub struct NetworkConfig {
    /// This node's id (sent in the hello).
    pub node_id: NodeId,
    /// Cluster port address (`host:port`) of every peer. For Raft RPCs a
    /// target missing here is dialed at its openraft `BasicNode::addr`;
    /// forwarding needs an entry.
    pub peers: BTreeMap<NodeId, String>,
    /// Mutual TLS (see [`crate::tls`]); `None` means plaintext, which the
    /// caller must have been configured for explicitly.
    pub tls: Option<Arc<ClientConfig>>,
    /// Maximum frame payload, both directions.
    pub max_frame: usize,
    /// TCP connect + TLS handshake + hello.
    pub connect_timeout: Duration,
    pub append_timeout: Duration,
    pub vote_timeout: Duration,
    /// Per snapshot chunk.
    pub snapshot_timeout: Duration,
    pub forward_timeout: Duration,
    /// Reconnect backoff after the first failed dial, doubled per further
    /// failure up to `backoff_max`.
    pub backoff_min: Duration,
    pub backoff_max: Duration,
    /// Requests awaiting an answer on one connection; more fail at once.
    pub max_in_flight: usize,
    /// Lower bound of the stall detection (see the module docs): a
    /// connection is closed once requests have been outstanding with no
    /// response for longer than `max(stall_timeout, 3 × request timeout)`
    /// and a request goes unanswered.
    pub stall_timeout: Duration,
    /// Encoded size above which an AppendEntries batch of more than one
    /// entry is split (`PayloadTooLarge`). Also capped by `max_frame`.
    pub append_budget: usize,
    /// This node's `-z`, sent in the hello; a peer with another value is
    /// refused (by the peer's listener, and here).
    pub max_job_size: u32,
}

impl NetworkConfig {
    /// Defaults: 1 s connect, 1 s append / vote, 10 s per snapshot chunk,
    /// 2 s forward, backoff 50 ms .. 2 s, 1024 requests in flight, 1 s
    /// stall timeout, 1 MiB append budget, the default `-z`.
    pub fn new(
        node_id: NodeId,
        peers: BTreeMap<NodeId, String>,
        tls: Option<Arc<ClientConfig>>,
    ) -> Self {
        NetworkConfig {
            node_id,
            peers,
            tls,
            max_frame: wire::DEFAULT_MAX_FRAME,
            connect_timeout: Duration::from_secs(1),
            append_timeout: Duration::from_secs(1),
            vote_timeout: Duration::from_secs(1),
            snapshot_timeout: Duration::from_secs(10),
            forward_timeout: Duration::from_secs(2),
            backoff_min: Duration::from_millis(50),
            backoff_max: Duration::from_secs(2),
            max_in_flight: 1024,
            stall_timeout: Duration::from_secs(1),
            append_budget: DEFAULT_APPEND_BUDGET,
            max_job_size: bstk_proto::DEFAULT_MAX_JOB_SIZE,
        }
    }

    fn backoff_delay(&self, failures: u32) -> Duration {
        let shift = failures.saturating_sub(1).min(20);
        self.backoff_min
            .saturating_mul(1u32 << shift)
            .min(self.backoff_max)
    }
}

/// The TCP/TLS cluster network (see the module docs). Cheap to clone.
#[derive(Clone)]
pub struct Network {
    inner: Arc<Inner>,
}

struct Inner {
    cfg: NetworkConfig,
    peers: StdMutex<HashMap<NodeId, Arc<Peer>>>,
    next_id: AtomicU64,
}

struct Peer {
    target: NodeId,
    addr: String,
    state: Mutex<DialState>,
    /// When the last response from `target` arrived (any request kind).
    last_response: Arc<StdMutex<Option<std::time::Instant>>>,
}

#[derive(Default)]
struct DialState {
    conn: Option<Arc<Conn>>,
    failures: u32,
    retry_at: Option<Instant>,
}

type Pending = Arc<StdMutex<HashMap<u64, oneshot::Sender<RpcResponse>>>>;

struct Conn {
    tx: mpsc::Sender<Vec<u8>>,
    pending: Pending,
    dead: Arc<AtomicBool>,
    kill: Arc<Notify>,
    /// Since when requests have been outstanding on this connection without
    /// any response arriving (`None` while nothing is outstanding). Set when
    /// a request is registered with nothing outstanding, moved forward by
    /// every response, cleared by a response that leaves nothing pending.
    stalled_since: Arc<StdMutex<Option<std::time::Instant>>>,
}

impl Conn {
    fn is_dead(&self) -> bool {
        self.dead.load(Ordering::Acquire)
    }

    /// Closes the connection if it has been stalled for longer than
    /// `bound` (called when a request went unanswered).
    fn check_stall(&self, bound: Duration) {
        let stalled = lock(&self.stalled_since).is_some_and(|t| t.elapsed() > bound);
        if stalled && !self.dead.swap(true, Ordering::AcqRel) {
            tracing::debug!(?bound, "cluster connection stalled; closing");
            self.kill.notify_one();
        }
    }
}

/// A registered request. Dropping it before [`Slot::answered`] (a timeout,
/// or openraft dropping the call) removes the pending entry, so a late
/// answer is discarded, and checks whether the connection is stalled.
struct Slot<'a> {
    conn: &'a Conn,
    id: u64,
    stall_bound: Duration,
    answered: bool,
}

impl Slot<'_> {
    fn answered(mut self) {
        self.answered = true;
    }
}

impl Drop for Slot<'_> {
    fn drop(&mut self) {
        if self.answered {
            return;
        }
        let was_pending = lock(&self.conn.pending).remove(&self.id).is_some();
        if was_pending {
            self.conn.check_stall(self.stall_bound);
        }
    }
}

/// Why a call failed, before mapping to openraft's or forwarding's errors.
#[derive(Debug)]
enum CallError {
    Unreachable(String),
    Timeout(Duration),
    Network(String),
    /// The encoded request (`len` bytes) exceeds the limit it was encoded
    /// with.
    TooLarge {
        len: usize,
    },
}

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

fn lock<T>(m: &StdMutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Network {
    pub fn new(cfg: NetworkConfig) -> Self {
        Network {
            inner: Arc::new(Inner {
                cfg,
                peers: StdMutex::new(HashMap::new()),
                next_id: AtomicU64::new(1),
            }),
        }
    }

    pub fn config(&self) -> &NetworkConfig {
        &self.inner.cfg
    }

    /// The shared connection slot for `target`; created on first use with
    /// the configured address, or `fallback` if none is configured.
    fn peer(&self, target: NodeId, fallback: Option<&str>) -> Option<Arc<Peer>> {
        let mut peers = lock(&self.inner.peers);
        if let Some(p) = peers.get(&target) {
            return Some(p.clone());
        }
        let addr = match self.inner.cfg.peers.get(&target) {
            Some(a) => a.clone(),
            None => fallback?.to_string(),
        };
        let p = Arc::new(Peer {
            target,
            addr,
            state: Mutex::new(DialState::default()),
            last_response: Arc::new(StdMutex::new(None)),
        });
        peers.insert(target, p.clone());
        Some(p)
    }

    /// When the last response of any kind (Raft RPC, forward, control)
    /// arrived from `target` over this network, if ever. On a leader the
    /// heartbeats make this a liveness signal for every follower.
    pub fn last_response(&self, target: NodeId) -> Option<std::time::Instant> {
        let p = lock(&self.inner.peers).get(&target).cloned()?;
        *lock(&p.last_response)
    }

    /// Requests awaiting an answer on the current connection to `target`.
    #[cfg(test)]
    pub(crate) fn pending_len(&self, target: NodeId) -> usize {
        let Some(p) = lock(&self.inner.peers).get(&target).cloned() else {
            return 0;
        };
        let st = p.state.try_lock();
        st.ok()
            .and_then(|st| st.conn.as_ref().map(|c| lock(&c.pending).len()))
            .unwrap_or(0)
    }

    /// Sends a control request to `target` and waits for its answer (like
    /// [`Network::forward`], no automatic resend).
    pub async fn control(
        &self,
        target: NodeId,
        req: ControlRequest,
    ) -> Result<ControlResponse, ForwardError> {
        let Some(peer) = self.peer(target, None) else {
            return Err(ForwardError::Unreachable(format!(
                "node {target} has no configured address"
            )));
        };
        let t = self.inner.cfg.forward_timeout;
        match self
            .call(&peer, RpcRequest::Control(req), t, self.inner.cfg.max_frame)
            .await
        {
            Ok(RpcResponse::Control(Ok(r))) => Ok(r),
            Ok(RpcResponse::Control(Err(e))) => {
                Err(ForwardError::Rejected(wire::sanitize(&e.to_string())))
            }
            Ok(_) => Err(ForwardError::Network("unexpected response kind".into())),
            Err(CallError::Unreachable(m)) => Err(ForwardError::Unreachable(m)),
            Err(CallError::Timeout(_)) => Err(ForwardError::Timeout),
            Err(CallError::Network(m)) => Err(ForwardError::Network(m)),
            Err(CallError::TooLarge { .. }) => Err(ForwardError::Rejected(
                "request exceeds the maximum frame size".into(),
            )),
        }
    }

    /// Sends `req` to `target` (see [`ForwardTransport::forward`]).
    pub async fn forward(
        &self,
        target: NodeId,
        req: ForwardRequest,
    ) -> Result<ForwardResponse, ForwardError> {
        let Some(peer) = self.peer(target, None) else {
            return Err(ForwardError::Unreachable(format!(
                "node {target} has no configured address"
            )));
        };
        let t = self.inner.cfg.forward_timeout;
        match self
            .call(&peer, RpcRequest::Forward(req), t, self.inner.cfg.max_frame)
            .await
        {
            Ok(RpcResponse::Forward(Ok(r))) => Ok(r),
            Ok(RpcResponse::Forward(Err(e))) => {
                Err(ForwardError::Rejected(wire::sanitize(&e.to_string())))
            }
            Ok(_) => Err(ForwardError::Network("unexpected response kind".into())),
            Err(CallError::Unreachable(m)) => Err(ForwardError::Unreachable(m)),
            Err(CallError::Timeout(_)) => Err(ForwardError::Timeout),
            Err(CallError::Network(m)) => Err(ForwardError::Network(m)),
            Err(CallError::TooLarge { .. }) => Err(ForwardError::Rejected(
                "request exceeds the maximum frame size".into(),
            )),
        }
    }

    /// One request: encodes it (at most `limit` bytes of payload), sends
    /// it on the shared connection and waits up to `timeout` for the
    /// answer. See the module docs for what a timeout does.
    async fn call(
        &self,
        peer: &Peer,
        body: RpcRequest,
        timeout: Duration,
        limit: usize,
    ) -> Result<RpcResponse, CallError> {
        let cfg = &self.inner.cfg;
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let frame = wire::encode(&ClientMsg::Request { id, body }, limit.min(cfg.max_frame))
            .map_err(|e| match e {
                FrameError::TooLarge { len, .. } => CallError::TooLarge { len },
                e => CallError::Network(e.to_string()),
            })?;
        let conn = self.connect(peer).await?;

        let (tx, rx) = oneshot::channel();
        {
            let mut pending = lock(&conn.pending);
            if pending.len() >= cfg.max_in_flight {
                return Err(CallError::Network("too many requests in flight".into()));
            }
            pending.insert(id, tx);
            let mut since = lock(&conn.stalled_since);
            if since.is_none() {
                *since = Some(std::time::Instant::now());
            }
        }
        let slot = Slot {
            conn: &conn,
            id,
            stall_bound: cfg.stall_timeout.max(timeout.saturating_mul(3)),
            answered: false,
        };
        // The connection task marks the connection dead before it drops the
        // pending senders, so a request registered after that is seen here.
        if conn.is_dead() {
            return Err(CallError::Network("connection closed".into()));
        }
        let exchange = async {
            conn.tx
                .send(frame)
                .await
                .map_err(|_| CallError::Network("connection closed".into()))?;
            rx.await
                .map_err(|_| CallError::Network("connection closed".into()))
        };
        match tokio::time::timeout(timeout, exchange).await {
            Ok(r) => {
                slot.answered();
                r
            }
            // `slot` is dropped here: the pending entry goes, and the
            // connection is closed only if it is stalled.
            Err(_) => Err(CallError::Timeout(timeout)),
        }
    }

    /// The live connection to `peer`, dialing if needed.
    async fn connect(&self, peer: &Peer) -> Result<Arc<Conn>, CallError> {
        let cfg = &self.inner.cfg;
        let mut st = peer.state.lock().await;
        if let Some(c) = &st.conn {
            if !c.is_dead() {
                return Ok(c.clone());
            }
            st.conn = None;
        }
        if let Some(at) = st.retry_at
            && Instant::now() < at
        {
            return Err(CallError::Unreachable(format!(
                "node {}: backing off after {} failed dial(s)",
                peer.target, st.failures
            )));
        }
        let dialed = tokio::time::timeout(cfg.connect_timeout, self.dial(peer)).await;
        let err = match dialed {
            Ok(Ok(io)) => {
                st.failures = 0;
                st.retry_at = None;
                let c = spawn_conn(io, cfg, peer.target, peer.last_response.clone());
                st.conn = Some(c.clone());
                return Ok(c);
            }
            Ok(Err(e)) => e,
            Err(_) => format!("connect timed out after {:?}", cfg.connect_timeout),
        };
        st.failures = st.failures.saturating_add(1);
        st.retry_at = Some(Instant::now() + cfg.backoff_delay(st.failures));
        tracing::debug!(target_node = peer.target, addr = %peer.addr, error = %err, "cluster dial failed");
        Err(CallError::Unreachable(format!(
            "node {} at {}: {err}",
            peer.target, peer.addr
        )))
    }

    async fn dial(&self, peer: &Peer) -> Result<Box<dyn Io>, String> {
        let cfg = &self.inner.cfg;
        let tcp = TcpStream::connect(&peer.addr)
            .await
            .map_err(|e| format!("connect: {e}"))?;
        tcp.set_nodelay(true)
            .map_err(|e| format!("set_nodelay: {e}"))?;
        let mut io: Box<dyn Io> = match &cfg.tls {
            None => Box::new(tcp),
            Some(tls) => {
                let name = crate::tls::server_name_for(peer.target)?;
                let s = TlsConnector::from(tls.clone())
                    .connect(name, tcp)
                    .await
                    .map_err(|e| format!("TLS handshake: {e}"))?;
                Box::new(s)
            }
        };
        let hello = ClientMsg::Hello(Hello {
            version: PROTOCOL_VERSION,
            from: cfg.node_id,
            to: peer.target,
            max_job_size: cfg.max_job_size,
        });
        let frame = wire::encode(&hello, cfg.max_frame).map_err(|e| e.to_string())?;
        wire::write_frame(&mut io, &frame)
            .await
            .map_err(|e| format!("hello: {e}"))?;
        match wire::read_frame::<_, ServerMsg>(&mut io, cfg.max_frame).await {
            Ok(Some(ServerMsg::Hello(ServerHello::Accepted {
                version,
                node_id,
                max_job_size,
            }))) if version == PROTOCOL_VERSION && node_id == peer.target => {
                if max_job_size == cfg.max_job_size {
                    Ok(io)
                } else {
                    let m = format!(
                        "max_job_size mismatch: node {node_id} uses {max_job_size}, \
                         this node uses {} (every node must use the same -z)",
                        cfg.max_job_size
                    );
                    tracing::error!(target_node = node_id, "{m}");
                    Err(m)
                }
            }
            Ok(Some(ServerMsg::Hello(ServerHello::Accepted {
                version, node_id, ..
            }))) => Err(format!(
                "peer answered as node {node_id} with protocol version {version}"
            )),
            Ok(Some(ServerMsg::Hello(ServerHello::Rejected { reason }))) => {
                // Peer-supplied text: escaped and truncated before use.
                let reason = wire::sanitize(&reason);
                if reason.starts_with("max_job_size mismatch") {
                    tracing::error!(
                        target_node = peer.target,
                        "cluster peer refused this node: {reason}"
                    );
                }
                Err(format!("rejected: {reason}"))
            }
            Ok(Some(ServerMsg::Response { .. })) => Err("response before hello".into()),
            Ok(None) => Err("connection closed during hello".into()),
            Err(e) => Err(format!("hello: {e}")),
        }
    }

    /// The backoff openraft applies after `Unreachable`.
    fn backoff_iter(&self) -> Backoff {
        let cfg = self.inner.cfg.clone();
        Backoff::new((1u32..).map(move |n| cfg.backoff_delay(n)))
    }
}

fn spawn_conn(
    io: Box<dyn Io>,
    cfg: &NetworkConfig,
    target: NodeId,
    last_response: Arc<StdMutex<Option<std::time::Instant>>>,
) -> Arc<Conn> {
    let (tx, rx) = mpsc::channel(cfg.max_in_flight.max(1));
    let conn = Arc::new(Conn {
        tx,
        pending: Arc::new(StdMutex::new(HashMap::new())),
        dead: Arc::new(AtomicBool::new(false)),
        kill: Arc::new(Notify::new()),
        stalled_since: Arc::new(StdMutex::new(None)),
    });
    let pending = conn.pending.clone();
    let dead = conn.dead.clone();
    let kill = conn.kill.clone();
    let stalled_since = conn.stalled_since.clone();
    let max_frame = cfg.max_frame;
    tokio::spawn(async move {
        let reason = run_conn(
            io,
            rx,
            &pending,
            &kill,
            max_frame,
            &last_response,
            &stalled_since,
        )
        .await;
        tracing::debug!(target_node = target, %reason, "cluster connection closed");
        dead.store(true, Ordering::Release);
        lock(&pending).clear();
    });
    conn
}

async fn run_conn(
    io: Box<dyn Io>,
    mut rx: mpsc::Receiver<Vec<u8>>,
    pending: &Pending,
    kill: &Notify,
    max_frame: usize,
    last_response: &StdMutex<Option<std::time::Instant>>,
    stalled_since: &StdMutex<Option<std::time::Instant>>,
) -> String {
    let (mut r, mut w) = tokio::io::split(io);
    let reader = async {
        loop {
            match wire::read_frame::<_, ServerMsg>(&mut r, max_frame).await {
                Ok(Some(ServerMsg::Response { id, body })) => {
                    let now = std::time::Instant::now();
                    *lock(last_response) = Some(now);
                    // Absent if the caller timed out; the answer is dropped.
                    let mut p = lock(pending);
                    let tx = p.remove(&id);
                    // Any response is progress (a late one included).
                    *lock(stalled_since) = if p.is_empty() { None } else { Some(now) };
                    drop(p);
                    if let Some(tx) = tx {
                        let _ = tx.send(body);
                    }
                }
                Ok(Some(ServerMsg::Hello(_))) => return "unexpected hello".to_string(),
                Ok(None) => return "closed by peer".to_string(),
                Err(e) => return e.to_string(),
            }
        }
    };
    let writer = async {
        while let Some(frame) = rx.recv().await {
            if let Err(e) = wire::write_frame(&mut w, &frame).await {
                return e.to_string();
            }
        }
        "no longer used".to_string()
    };
    tokio::select! {
        r = reader => r,
        r = writer => r,
        () = kill.notified() => "closed: stalled".to_string(),
    }
}

impl Network {
    /// Asks `target` for its durable Raft state (a status probe, answered
    /// even before its Raft runs). Uses the forward timeout.
    pub async fn status(&self, target: NodeId) -> Result<NodeStatus, ForwardError> {
        let Some(peer) = self.peer(target, None) else {
            return Err(ForwardError::Unreachable(format!(
                "node {target} has no configured address"
            )));
        };
        let t = self.inner.cfg.forward_timeout;
        match self
            .call(&peer, RpcRequest::Status, t, self.inner.cfg.max_frame)
            .await
        {
            Ok(RpcResponse::Control(Err(e))) => {
                Err(ForwardError::Rejected(wire::sanitize(&e.to_string())))
            }
            Ok(r) => r
                .into_status()
                .ok_or_else(|| ForwardError::Network("unexpected response kind".into())),
            Err(CallError::Unreachable(m)) => Err(ForwardError::Unreachable(m)),
            Err(CallError::Timeout(_)) => Err(ForwardError::Timeout),
            Err(CallError::Network(m)) => Err(ForwardError::Network(m)),
            Err(CallError::TooLarge { .. }) => Err(ForwardError::Rejected(
                "request exceeds the maximum frame size".into(),
            )),
        }
    }
}

impl StatusTransport for Network {
    async fn status(&self, target: NodeId) -> Result<NodeStatus, ForwardError> {
        Network::status(self, target).await
    }
}

impl ForwardTransport for Network {
    async fn forward(
        &self,
        target: NodeId,
        req: ForwardRequest,
    ) -> Result<ForwardResponse, ForwardError> {
        Network::forward(self, target, req).await
    }
}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = PeerClient;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> PeerClient {
        PeerClient {
            net: self.clone(),
            target,
            peer: self.peer(target, Some(&node.addr)),
        }
    }
}

/// openraft's per-target client; shares the target's connection with every
/// other user of the same [`Network`].
pub struct PeerClient {
    net: Network,
    target: NodeId,
    peer: Option<Arc<Peer>>,
}

type RpcErr<E = openraft::error::Infallible> = RPCError<NodeId, BasicNode, RaftError<NodeId, E>>;

impl PeerClient {
    async fn rpc(
        &self,
        body: RpcRequest,
        timeout: Duration,
        limit: usize,
    ) -> Result<RpcResponse, CallError> {
        let Some(peer) = &self.peer else {
            return Err(CallError::Unreachable(format!(
                "node {} has no address",
                self.target
            )));
        };
        self.net.call(peer, body, timeout, limit).await
    }

    fn call_err<E: std::error::Error>(&self, action: RPCTypes, e: CallError) -> RpcErr<E> {
        match e {
            CallError::Unreachable(m) => {
                RPCError::Unreachable(Unreachable::new(&io::Error::other(m)))
            }
            CallError::Timeout(t) => RPCError::Timeout(Timeout {
                action,
                id: self.net.inner.cfg.node_id,
                target: self.target,
                timeout: t,
            }),
            CallError::Network(m) => network_err(m),
            CallError::TooLarge { len } => network_err(format!(
                "{action} of {len} bytes exceeds the maximum frame size"
            )),
        }
    }

    /// A remote `Fatal` becomes a `RemoteError`; anything else (a listener
    /// rejection, or a snapshot mismatch outside a snapshot) is a network
    /// error.
    fn remote_err<E: std::error::Error>(&self, e: WireError) -> RpcErr<E> {
        match e {
            WireError::Fatal(WireFatal::Storage(m)) => RPCError::RemoteError(RemoteError::new(
                self.target,
                RaftError::Fatal(WireFatal::Storage(wire::sanitize(&m)).into_fatal()),
            )),
            WireError::Fatal(f) => RPCError::RemoteError(RemoteError::new(
                self.target,
                RaftError::Fatal(f.into_fatal()),
            )),
            other => network_err(wire::sanitize(&other.to_string())),
        }
    }
}

fn network_err<E: std::error::Error>(m: String) -> RPCError<NodeId, BasicNode, E> {
    RPCError::Network(NetworkError::new(&io::Error::other(m)))
}

/// How many of `n` entries (encoded in `len` bytes) to send so that the
/// batch fits in `budget` bytes, assuming entries of similar size: at least
/// one, and fewer than `n`.
fn entries_hint(n: u64, len: usize, budget: usize) -> u64 {
    let scaled = u128::from(n) * budget as u128 / (len.max(1) as u128);
    u64::try_from(scaled)
        .unwrap_or(u64::MAX)
        .clamp(1, n.saturating_sub(1).max(1))
}

fn effective(configured: Duration, option: &RPCOption) -> Duration {
    configured.min(option.hard_ttl())
}

impl RaftNetwork<TypeConfig> for PeerClient {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RpcErr> {
        let cfg = &self.net.inner.cfg;
        let n = rpc.entries.len() as u64;
        let t = effective(cfg.append_timeout, &option);
        // A single entry may use the whole frame; a batch only the budget.
        let limit = if n > 1 {
            cfg.append_budget.max(1)
        } else {
            cfg.max_frame
        };
        let budget = limit.min(cfg.max_frame);
        match self.rpc(RpcRequest::AppendEntries(rpc), t, limit).await {
            Ok(RpcResponse::AppendEntries(Ok(r))) => Ok(r),
            Ok(RpcResponse::AppendEntries(Err(e))) => Err(self.remote_err(e)),
            Ok(_) => Err(network_err("unexpected response kind".into())),
            // openraft retries with at most this many entries.
            Err(CallError::TooLarge { len }) if n > 1 => Err(RPCError::PayloadTooLarge(
                PayloadTooLarge::new_entries_hint(entries_hint(n, len, budget)),
            )),
            Err(e) => Err(self.call_err(RPCTypes::AppendEntries, e)),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, RpcErr<InstallSnapshotError>> {
        let cfg = &self.net.inner.cfg;
        let t = effective(cfg.snapshot_timeout, &option);
        match self
            .rpc(RpcRequest::InstallSnapshot(rpc), t, cfg.max_frame)
            .await
        {
            Ok(RpcResponse::InstallSnapshot(Ok(r))) => Ok(r),
            Ok(RpcResponse::InstallSnapshot(Err(WireError::SnapshotMismatch(m)))) => {
                Err(RPCError::RemoteError(RemoteError::new(
                    self.target,
                    RaftError::APIError(InstallSnapshotError::SnapshotMismatch(m)),
                )))
            }
            Ok(RpcResponse::InstallSnapshot(Err(e))) => Err(self.remote_err(e)),
            Ok(_) => Err(network_err("unexpected response kind".into())),
            Err(e) => Err(self.call_err(RPCTypes::InstallSnapshot, e)),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RpcErr> {
        let cfg = &self.net.inner.cfg;
        let t = effective(cfg.vote_timeout, &option);
        match self.rpc(RpcRequest::Vote(rpc), t, cfg.max_frame).await {
            Ok(RpcResponse::Vote(Ok(r))) => Ok(r),
            Ok(RpcResponse::Vote(Err(e))) => Err(self.remote_err(e)),
            Ok(_) => Err(network_err("unexpected response kind".into())),
            Err(e) => Err(self.call_err(RPCTypes::Vote, e)),
        }
    }

    fn backoff(&self) -> Backoff {
        self.net.backoff_iter()
    }
}
