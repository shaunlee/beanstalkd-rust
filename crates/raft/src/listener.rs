//! The accepting side of the cluster port (docs/DESIGN.md §8 "Listener").
//!
//! Per connection: TLS handshake (a client certificate from the cluster CA is
//! required), then the dialer's hello within `handshake_timeout`. The hello
//! must name this node as `to`, use this protocol version, come `from` a node
//! whose identity the connection proves (with TLS the certificate must be
//! valid for `bstk-node-<from>`, [`crate::tls::verify_peer_identity`]), and
//! that node must be allowed ([`PeerAllowlist`]: the effective membership once
//! the node has one, the config seeds before); otherwise the listener answers
//! `Rejected` and closes. No request is read before the hello is accepted.
//! Identity is checked before membership, so only an authenticated node
//! learns that it is not a member ([`REJECT_NOT_MEMBER`]).
//!
//! Requests on one connection are served strictly in order: openraft keeps one
//! request in flight per replication stream and the other users of a link are
//! light, so pipelining buys little, per-connection memory stays at one frame
//! (at most `max_frame`) plus one response, and the order of forwards from one
//! owner is preserved.
//!
//! Unauthenticated and authenticated connections use separate budgets so that
//! connections that never finish the handshake cannot keep the peers out:
//! handshakes are limited by `max_handshakes` and `max_handshakes_per_ip`, and
//! an authenticated connection holds its peer's single slot (a newer hello
//! from the same peer closes the older connection).
//!
//! Status probes ([`RpcRequest::Status`]) are answered from
//! [`ListenerConfig::status`] at any time. With
//! [`ClusterListener::spawn_deferred`] the Raft node and forward handler are
//! installed later through the returned [`ServiceSlot`]; until then Raft RPCs,
//! forwards and control requests are refused ([`NOT_STARTED`]); a Vote RPC
//! is checked against the vote gate first, so it is counted as refused by a
//! closed gate.
//!
//! Rejections are logged at most about once a second; a rejected hello gets a
//! generic reason ([`REJECT_HELLO`], [`REJECT_NOT_MEMBER`], [`REJECT_VERSION`],
//! [`REJECT_MAX_JOB_SIZE`]) and the details are only logged locally.

use std::collections::{BTreeSet, HashMap};
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::Duration;

use openraft::Raft;
use rustls::ServerConfig;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use tokio_rustls::TlsAcceptor;

use crate::forward::{ForwardHandler, check_control, check_forward};
use crate::status::StatusSource;
use crate::wire::{
    self, ClientMsg, FrameError, PROTOCOL_VERSION, RpcRequest, RpcResponse, ServerHello, ServerMsg,
    WireError,
};
use crate::{NodeId, TypeConfig};
use bstk_net::QuietTcp;

/// Keeps this node out of elections (see [`ListenerConfig::vote_gate`]): while
/// closed, every inbound Vote RPC is answered with [`WireError::Rejected`]
/// without reaching Raft (no vote is granted, the term is not updated) and
/// counted. A rejoining node keeps it closed until it has caught up
/// (docs/DESIGN.md §8 "Rejoin"). Other requests are unaffected.
/// `VoteGate::default()` is closed.
#[derive(Debug, Default)]
pub struct VoteGate {
    open: AtomicBool,
    refused: AtomicU64,
}

impl VoteGate {
    /// A gate that is initially open (`true`) or closed (`false`).
    pub fn new(open: bool) -> Self {
        VoteGate {
            open: AtomicBool::new(open),
            refused: AtomicU64::new(0),
        }
    }

    pub fn open(&self) {
        self.open.store(true, Ordering::Release);
    }

    pub fn close(&self) {
        self.open.store(false, Ordering::Release);
    }

    pub fn is_open(&self) -> bool {
        self.open.load(Ordering::Acquire)
    }

    pub fn refused(&self) -> u64 {
        self.refused.load(Ordering::Relaxed)
    }

    /// Whether a vote may go to Raft; counts a refusal if not.
    pub(crate) fn admit(&self) -> bool {
        if self.is_open() {
            return true;
        }
        self.refused.fetch_add(1, Ordering::Relaxed);
        false
    }
}

pub const VOTE_GATE_CLOSED: &str = "this node does not vote yet";

#[derive(Clone)]
pub struct ListenerConfig {
    pub node_id: NodeId,
    /// Node ids allowed to connect at first (the config seeds); replaced
    /// at runtime through [`ClusterListener::allowlist`].
    pub peers: BTreeSet<NodeId>,
    /// Mutual TLS (see [`crate::tls`]); `None` means plaintext, which the
    /// caller must have been configured for explicitly.
    pub tls: Option<Arc<ServerConfig>>,
    pub max_frame: usize,
    /// Inbound connections in the TLS handshake or hello at once.
    pub max_handshakes: usize,
    /// Of those, from one source IP address. A floor: an allowlist of `n`
    /// peers raises it to `2 × n` ([`PeerAllowlist::set`]).
    pub max_handshakes_per_ip: usize,
    /// TLS handshake plus hello.
    pub handshake_timeout: Duration,
    /// This node's `-z`; hellos with another value are rejected.
    pub max_job_size: u32,
    /// When present and closed, inbound Vote RPCs are refused (see
    /// [`VoteGate`]). `None` (the default) serves every vote.
    pub vote_gate: Option<Arc<VoteGate>>,
    /// Answers status probes (the node's log store). `None` (the default):
    /// probes are refused.
    pub status: Option<Arc<dyn StatusSource>>,
}

impl ListenerConfig {
    /// Defaults: 16 handshakes at once, `max(4, 2 × peers)` of them per
    /// source address (every peer may share one address, as on a test
    /// machine or behind NAT), 2 s handshake, 32 MiB frames, the default
    /// `-z`, no vote gate.
    pub fn new(node_id: NodeId, peers: BTreeSet<NodeId>, tls: Option<Arc<ServerConfig>>) -> Self {
        let per_ip = peers.len().saturating_mul(2).max(4);
        ListenerConfig {
            node_id,
            peers,
            tls,
            max_frame: wire::DEFAULT_MAX_FRAME,
            max_handshakes: 16,
            max_handshakes_per_ip: per_ip,
            handshake_timeout: Duration::from_secs(2),
            max_job_size: bstk_proto::DEFAULT_MAX_JOB_SIZE,
            vote_gate: None,
            status: None,
        }
    }
}

/// A running cluster listener. Dropping it (or [`shutdown`]) stops
/// accepting and closes every connection it accepted.
///
/// [`shutdown`]: ClusterListener::shutdown
pub struct ClusterListener {
    local_addr: SocketAddr,
    task: Option<JoinHandle<()>>,
    shared: Arc<Shared>,
}

/// Replaces the set of nodes a listener accepts (see the module docs).
/// Cloneable; keeps the listener's bookkeeping alive, not its task.
#[derive(Clone)]
pub struct PeerAllowlist {
    shared: Arc<Shared>,
}

impl PeerAllowlist {
    /// From now on only `peers` may connect: hellos from other nodes get
    /// [`REJECT_NOT_MEMBER`], and the live connection of every node not in
    /// `peers` is closed. The per-address handshake budget becomes
    /// `max(configured, 2 × peers)`.
    pub fn set(&self, peers: BTreeSet<NodeId>) {
        let sh = &self.shared;
        let per_ip = sh
            .cfg
            .max_handshakes_per_ip
            .max(peers.len().saturating_mul(2));
        sh.per_ip_limit.store(per_ip, Ordering::Relaxed);
        let mut allowed = lock(&sh.allowed);
        // Under the `allowed` lock, as `register` checks it: a connection
        // registered concurrently is either refused there or closed here.
        for (&peer, (_, close)) in lock(&sh.peers).iter() {
            if !peers.contains(&peer) {
                tracing::warn!(
                    peer,
                    "cluster peer is no longer allowed: closing its connection"
                );
                close.notify_one();
            }
        }
        *allowed = peers;
    }

    pub fn get(&self) -> BTreeSet<NodeId> {
        lock(&self.shared.allowed).clone()
    }

    /// The current per-address handshake budget.
    pub fn handshakes_per_ip(&self) -> usize {
        self.shared.per_ip_limit.load(Ordering::Relaxed)
    }
}

struct Service<H> {
    raft: Raft<TypeConfig>,
    handler: Arc<H>,
}

/// Installs the service of a listener started with
/// [`ClusterListener::spawn_deferred`].
pub struct ServiceSlot<H> {
    slot: Arc<OnceLock<Service<H>>>,
}

impl<H: ForwardHandler> ServiceSlot<H> {
    /// From now on Raft RPCs go to `raft`, forwards and control requests to
    /// `handler`. Only the first call has an effect (returns `false`
    /// otherwise).
    pub fn set(&self, raft: Raft<TypeConfig>, handler: Arc<H>) -> bool {
        self.slot.set(Service { raft, handler }).is_ok()
    }
}

/// The reason sent for a request that needs Raft before it runs.
pub const NOT_STARTED: &str = "raft is not running on this node yet";

/// The reason sent for a status probe to a listener without a status
/// source.
pub const NO_STATUS: &str = "status probes are not served by this node";

impl ClusterListener {
    pub fn spawn<H: ForwardHandler>(
        listener: TcpListener,
        cfg: ListenerConfig,
        raft: Raft<TypeConfig>,
        handler: Arc<H>,
    ) -> io::Result<Self> {
        let (l, slot) = Self::spawn_deferred(listener, cfg)?;
        slot.set(raft, handler);
        Ok(l)
    }

    /// Serves `listener` before Raft runs: status probes are answered at
    /// once, everything else once the service is installed through the
    /// returned slot (see the module docs).
    pub fn spawn_deferred<H: ForwardHandler>(
        listener: TcpListener,
        cfg: ListenerConfig,
    ) -> io::Result<(Self, ServiceSlot<H>)> {
        let local_addr = listener.local_addr()?;
        let slot = Arc::new(OnceLock::new());
        let shared = Arc::new(Shared {
            handshakes: Arc::new(Semaphore::new(cfg.max_handshakes)),
            per_ip_limit: AtomicUsize::new(cfg.max_handshakes_per_ip),
            allowed: StdMutex::new(cfg.peers.clone()),
            cfg,
            per_ip: StdMutex::new(HashMap::new()),
            peers: StdMutex::new(HashMap::new()),
            next_gen: AtomicU64::new(1),
            rejects: RateLimit::new(),
        });
        let task = tokio::spawn(accept_loop(listener, shared.clone(), slot.clone()));
        Ok((
            ClusterListener {
                local_addr,
                task: Some(task),
                shared,
            },
            ServiceSlot { slot },
        ))
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// The handle that replaces the set of nodes this listener accepts.
    pub fn allowlist(&self) -> PeerAllowlist {
        PeerAllowlist {
            shared: self.shared.clone(),
        }
    }

    pub async fn shutdown(mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for ClusterListener {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

/// Generic reason sent for a rejected hello (wrong target, certificate not
/// valid for the claimed id).
pub const REJECT_HELLO: &str = "hello rejected";
/// Reason sent to an authenticated node that is not allowed (not in the
/// effective membership, or not a config seed before this node has a
/// membership); the dialer logs it as such.
pub const REJECT_NOT_MEMBER: &str = "not a member of this cluster";
/// Reason sent for a hello with another protocol version.
pub const REJECT_VERSION: &str = "unsupported protocol version";
/// Reason sent for a hello with another `-z` (the dialer logs it as a
/// configuration error).
pub const REJECT_MAX_JOB_SIZE: &str = "max_job_size mismatch (every node must use the same -z)";

struct RateLimit {
    state: StdMutex<(Option<std::time::Instant>, u64)>,
}

impl RateLimit {
    const INTERVAL: Duration = Duration::from_secs(1);

    fn new() -> Self {
        RateLimit {
            state: StdMutex::new((None, 0)),
        }
    }

    /// `Some(suppressed since the last logged one)` if this one may be
    /// logged.
    fn allow(&self) -> Option<u64> {
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let now = std::time::Instant::now();
        if st.0.is_some_and(|t| now.duration_since(t) < Self::INTERVAL) {
            st.1 = st.1.saturating_add(1);
            return None;
        }
        let suppressed = st.1;
        *st = (Some(now), 0);
        Some(suppressed)
    }
}

struct Shared {
    cfg: ListenerConfig,
    handshakes: Arc<Semaphore>,
    /// Handshakes allowed per source address (see [`PeerAllowlist::set`]).
    per_ip_limit: AtomicUsize,
    /// Nodes allowed to connect. Lock order: `allowed`, then `peers`.
    allowed: StdMutex<BTreeSet<NodeId>>,
    per_ip: StdMutex<HashMap<IpAddr, usize>>,
    /// The authenticated connection of each peer: generation and closer.
    peers: StdMutex<HashMap<NodeId, (u64, Arc<Notify>)>>,
    next_gen: AtomicU64,
    rejects: RateLimit,
}

fn lock<T>(m: &StdMutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Shared {
    fn log_reject(&self, addr: SocketAddr, what: &str) {
        if let Some(suppressed) = self.rejects.allow() {
            tracing::warn!(%addr, suppressed, "cluster connection rejected: {what}");
        }
    }

    /// Makes `peer`'s new connection the current one, closing the older
    /// one; returns its generation and the notification that closes it, or
    /// `None` if `peer` is no longer allowed (the allowlist changed after
    /// its hello was accepted).
    fn register(&self, peer: NodeId) -> Option<(u64, Arc<Notify>)> {
        let allowed = lock(&self.allowed);
        if !allowed.contains(&peer) {
            return None;
        }
        let generation = self.next_gen.fetch_add(1, Ordering::Relaxed);
        let close = Arc::new(Notify::new());
        let old = lock(&self.peers).insert(peer, (generation, close.clone()));
        drop(allowed);
        if let Some((_, old)) = old {
            old.notify_one();
        }
        Some((generation, close))
    }

    fn is_allowed(&self, peer: NodeId) -> bool {
        lock(&self.allowed).contains(&peer)
    }

    fn unregister(&self, peer: NodeId, generation: u64) {
        let mut peers = lock(&self.peers);
        if peers.get(&peer).is_some_and(|(g, _)| *g == generation) {
            peers.remove(&peer);
        }
    }
}

struct HandshakeSlot {
    shared: Arc<Shared>,
    ip: IpAddr,
    _permit: OwnedSemaphorePermit,
}

impl HandshakeSlot {
    fn acquire(shared: &Arc<Shared>, ip: IpAddr) -> Result<Self, &'static str> {
        let permit = shared
            .handshakes
            .clone()
            .try_acquire_owned()
            .map_err(|_| "too many handshakes in progress")?;
        let mut per_ip = lock(&shared.per_ip);
        let n = per_ip.entry(ip).or_insert(0);
        if *n >= shared.per_ip_limit.load(Ordering::Relaxed) {
            return Err("too many handshakes in progress from this address");
        }
        *n += 1;
        Ok(HandshakeSlot {
            shared: shared.clone(),
            ip,
            _permit: permit,
        })
    }
}

impl Drop for HandshakeSlot {
    fn drop(&mut self) {
        let mut per_ip = lock(&self.shared.per_ip);
        if let Some(n) = per_ip.get_mut(&self.ip) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                per_ip.remove(&self.ip);
            }
        }
    }
}

async fn accept_loop<H: ForwardHandler>(
    listener: TcpListener,
    shared: Arc<Shared>,
    service: Arc<OnceLock<Service<H>>>,
) {
    // Owned here so that aborting this task aborts every connection.
    let mut conns = JoinSet::new();
    loop {
        let accepted = tokio::select! {
            a = listener.accept() => a,
            Some(_) = conns.join_next(), if !conns.is_empty() => continue,
        };
        let (tcp, addr) = match accepted {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!(error = %e, "cluster accept failed");
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let slot = match HandshakeSlot::acquire(&shared, addr.ip()) {
            Ok(slot) => slot,
            Err(why) => {
                shared.log_reject(addr, why);
                drop(tcp);
                continue;
            }
        };
        let shared = shared.clone();
        let service = service.clone();
        conns.spawn(async move {
            serve_tcp(tcp, addr, slot, &shared, &service).await;
        });
    }
}

/// Handshake (TLS and hello) within the timeout, holding `slot`; then the
/// peer's connection slot until the connection ends or is replaced.
async fn serve_tcp<H: ForwardHandler>(
    tcp: TcpStream,
    addr: SocketAddr,
    slot: HandshakeSlot,
    shared: &Shared,
    service: &OnceLock<Service<H>>,
) {
    let cfg = &shared.cfg;
    let _ = tcp.set_nodelay(true);
    let tcp = match QuietTcp::new(tcp) {
        Ok(t) => t,
        Err(e) => {
            tracing::debug!(%addr, error = %e, "cluster connection: cannot register the socket");
            return;
        }
    };
    let result = match &cfg.tls {
        None => {
            let handshake = async {
                let mut io = tcp;
                let peer = hello(&mut io, shared, addr, |_| Ok(())).await?;
                Ok::<_, String>((io, peer))
            };
            match tokio::time::timeout(cfg.handshake_timeout, handshake).await {
                Ok(Ok((io, peer))) => {
                    drop(slot);
                    serve_peer(io, peer, shared, service).await
                }
                Ok(Err(e)) => Err(Phase::Handshake(e)),
                Err(_) => Err(Phase::Handshake("handshake timed out".into())),
            }
        }
        Some(tls) => {
            let acceptor = TlsAcceptor::from(tls.clone());
            let handshake = async {
                let mut io = acceptor
                    .accept(tcp)
                    .await
                    .map_err(|e| format!("TLS handshake: {e}"))?;
                let certs = io.get_ref().1.peer_certificates().map(<[_]>::to_vec);
                let peer = hello(&mut io, shared, addr, |id| {
                    crate::tls::verify_peer_identity(certs.as_deref(), id)
                })
                .await?;
                Ok::<_, String>((io, peer))
            };
            match tokio::time::timeout(cfg.handshake_timeout, handshake).await {
                Ok(Ok((io, peer))) => {
                    drop(slot);
                    serve_peer(io, peer, shared, service).await
                }
                Ok(Err(e)) => Err(Phase::Handshake(e)),
                Err(_) => Err(Phase::Handshake("handshake timed out".into())),
            }
        }
    };
    match result {
        Ok(()) => {}
        Err(Phase::Handshake(e)) => shared.log_reject(addr, &e),
        Err(Phase::Serve(e)) => tracing::debug!(%addr, error = %e, "cluster connection closed"),
    }
}

enum Phase {
    Handshake(String),
    Serve(String),
}

/// Serves an authenticated connection of `peer` as that peer's current
/// one, until it ends or a newer connection of the same peer replaces it.
async fn serve_peer<S, H>(
    io: S,
    peer: NodeId,
    shared: &Shared,
    service: &OnceLock<Service<H>>,
) -> Result<(), Phase>
where
    S: AsyncRead + AsyncWrite + Unpin,
    H: ForwardHandler,
{
    let Some((generation, close)) = shared.register(peer) else {
        return Err(Phase::Serve(format!(
            "node {peer} is no longer allowed (membership changed during its hello)"
        )));
    };
    let r = tokio::select! {
        r = serve(io, peer, &shared.cfg, service) => r,
        () = close.notified() => Err(if shared.is_allowed(peer) {
            "replaced by a newer connection of the same peer".into()
        } else {
            format!("node {peer} is no longer allowed (not a member)")
        }),
    };
    shared.unregister(peer, generation);
    r.map_err(Phase::Serve)
}

/// Reads and checks the hello; answers it. Returns the peer's id. The
/// answer to a rejected hello carries a generic reason; the details are
/// returned (and logged by the caller).
async fn hello<S, F>(
    io: &mut S,
    shared: &Shared,
    addr: SocketAddr,
    identity: F,
) -> Result<NodeId, String>
where
    S: AsyncRead + AsyncWrite + Unpin,
    F: FnOnce(NodeId) -> Result<(), String>,
{
    let cfg = &shared.cfg;
    let h = match wire::read_frame::<_, ClientMsg>(io, cfg.max_frame).await {
        Ok(Some(ClientMsg::Hello(h))) => h,
        Ok(Some(ClientMsg::Request { .. })) => return Err("request before hello".into()),
        Ok(None) => return Err("closed before hello".into()),
        Err(e) => return Err(format!("hello: {e}")),
    };
    // (details for the local log, generic reason for the peer). Identity
    // before membership: only a node that proved its id learns that it is
    // not a member.
    let verdict: Result<(), (String, &str)> = if h.version != PROTOCOL_VERSION {
        Err((
            format!("unsupported protocol version {}", h.version),
            REJECT_VERSION,
        ))
    } else if h.to != cfg.node_id {
        Err((
            format!("hello for node {}, this is node {}", h.to, cfg.node_id),
            REJECT_HELLO,
        ))
    } else if h.from == cfg.node_id {
        Err((
            format!("hello from this node's own id {}", h.from),
            REJECT_HELLO,
        ))
    } else if let Err(e) = identity(h.from) {
        Err((e, REJECT_HELLO))
    } else if !shared.is_allowed(h.from) {
        Err((
            format!("node {} is not a member of the cluster", h.from),
            REJECT_NOT_MEMBER,
        ))
    } else if h.max_job_size != cfg.max_job_size {
        let reason = format!(
            "max_job_size mismatch: node {} uses {}, node {} uses {} \
             (every node must use the same -z)",
            h.from, h.max_job_size, cfg.node_id, cfg.max_job_size
        );
        tracing::error!(from = h.from, %addr, "cluster hello rejected: {reason}");
        Err((reason, REJECT_MAX_JOB_SIZE))
    } else {
        Ok(())
    };
    let answer = match &verdict {
        Ok(()) => ServerHello::Accepted {
            version: PROTOCOL_VERSION,
            node_id: cfg.node_id,
            max_job_size: cfg.max_job_size,
        },
        Err((_, generic)) => ServerHello::Rejected {
            reason: (*generic).to_string(),
        },
    };
    let frame =
        wire::encode(&ServerMsg::Hello(answer), cfg.max_frame).map_err(|e| e.to_string())?;
    wire::write_frame(io, &frame)
        .await
        .map_err(|e| e.to_string())?;
    verdict
        .map(|()| h.from)
        .map_err(|(detail, _)| format!("hello from node {} rejected: {detail}", h.from))
}

async fn serve<S, H>(
    io: S,
    peer: NodeId,
    cfg: &ListenerConfig,
    service: &OnceLock<Service<H>>,
) -> Result<(), String>
where
    S: AsyncRead + AsyncWrite + Unpin,
    H: ForwardHandler,
{
    // Buffered reads (see `client::READ_BUF`); writes pass through.
    let mut io = tokio::io::BufReader::with_capacity(crate::client::READ_BUF, io);
    loop {
        let (id, body) = match wire::read_frame::<_, ClientMsg>(&mut io, cfg.max_frame).await {
            Ok(Some(ClientMsg::Request { id, body })) => (id, body),
            Ok(Some(ClientMsg::Hello(_))) => return Err("second hello".into()),
            Ok(None) => return Ok(()),
            Err(e) => return Err(e.to_string()),
        };
        let svc = service.get().map(|s| (&s.raft, &*s.handler));
        let resp = dispatch(
            peer,
            body,
            svc,
            cfg.vote_gate.as_deref(),
            cfg.status.as_deref(),
        )
        .await;
        let frame = match wire::encode(&ServerMsg::Response { id, body: resp }, cfg.max_frame) {
            Ok(f) => f,
            Err(FrameError::TooLarge { len, max }) => {
                return Err(format!("response of {len} bytes exceeds {max}"));
            }
            Err(e) => return Err(e.to_string()),
        };
        wire::write_frame(&mut io, &frame)
            .await
            .map_err(|e| e.to_string())?;
    }
}

/// Serves one request (shared with the simulated network). `service` is
/// `None` until Raft runs.
pub(crate) async fn dispatch<H: ForwardHandler>(
    peer: NodeId,
    body: RpcRequest,
    service: Option<(&Raft<TypeConfig>, &H)>,
    vote_gate: Option<&VoteGate>,
    status: Option<&dyn StatusSource>,
) -> RpcResponse {
    let not_started = || WireError::Rejected(NOT_STARTED.into());
    match body {
        RpcRequest::Status => match status {
            Some(s) => RpcResponse::status(s.status()),
            // (A status answer cannot carry an error; any other kind is a
            // failed probe for the dialer.)
            None => RpcResponse::Control(Err(WireError::Rejected(NO_STATUS.into()))),
        },
        RpcRequest::AppendEntries(r) => RpcResponse::AppendEntries(match service {
            Some((raft, _)) => raft
                .append_entries(r)
                .await
                .map_err(|e| WireError::from_raft(&e)),
            None => Err(not_started()),
        }),
        RpcRequest::Vote(r) => {
            if vote_gate.is_some_and(|g| !g.admit()) {
                tracing::debug!(from = peer, "vote refused: the vote gate is closed");
                return RpcResponse::Vote(Err(WireError::Rejected(VOTE_GATE_CLOSED.into())));
            }
            RpcResponse::Vote(match service {
                Some((raft, _)) => raft.vote(r).await.map_err(|e| WireError::from_raft(&e)),
                None => Err(not_started()),
            })
        }
        RpcRequest::InstallSnapshot(r) => RpcResponse::InstallSnapshot(match service {
            Some((raft, _)) => raft
                .install_snapshot(r)
                .await
                .map_err(|e| WireError::from_snapshot(&e)),
            None => Err(not_started()),
        }),
        RpcRequest::Forward(r) => RpcResponse::Forward(match (check_forward(peer, &r), service) {
            (Err(reason), _) => Err(WireError::Rejected(reason)),
            (Ok(()), Some((_, handler))) => Ok(handler.forward(r).await),
            (Ok(()), None) => Err(not_started()),
        }),
        RpcRequest::Control(r) => RpcResponse::Control(match (check_control(peer, &r), service) {
            (Err(reason), _) => Err(WireError::Rejected(reason)),
            (Ok(()), Some((_, handler))) => Ok(handler.control(r).await),
            (Ok(()), None) => Err(not_started()),
        }),
    }
}
