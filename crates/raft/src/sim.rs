//! In-process simulated cluster network, for tests only (compiled with
//! `cfg(test)` or the `sim` feature, which the server never enables).
//!
//! [`SimNetwork`] connects any number of `openraft::Raft` nodes in one
//! process. Every Raft RPC and every forward goes through the same request
//! path as the TCP listener ([`crate::listener`]'s dispatch), subject to
//! faults set on the network:
//!
//! - **partitions** per directed link ([`SimNetwork::block`]); a request
//!   over a blocked link fails at once as `Unreachable`, a response over a
//!   blocked link is lost (the caller times out, the request took effect);
//!   [`SimNetwork::partition`] / [`SimNetwork::isolate`] block both ways;
//! - **message loss**: each request and each response is dropped with
//!   probability `drop` (the caller times out);
//! - **delay**: each request waits a uniform delay in `delay` first;
//! - **duplication**: with probability `dup` the request is delivered a
//!   second time (concurrently, after its own delay; that answer is
//!   discarded);
//! - **paused nodes**: messages to and from a paused node wait until it is
//!   resumed (or the caller's timeout). Unlike SIGSTOP, the paused node's
//!   openraft core keeps running its timers; only its links stall.
//!
//! Every per-message decision is a pure function of the seed, the directed
//! link and the message's sequence number on that link, so a link's
//! decisions are reproducible from the seed regardless of how tokio
//! interleaves the links. Injected faults are recorded
//! ([`SimNetwork::fault_log`]). [`FaultSchedule::generate`] derives a timed
//! sequence of partitions, pauses and heals from a seed.
//!
//! Status probes ([`SimNode`]'s [`StatusTransport`]) are answered by the
//! node's status source ([`SimNetwork::set_status_source`]), which may be
//! set before the node's Raft is registered (a node probing its peers
//! before it starts Raft); links, pauses and faults apply as to any
//! request.
//!
//! [`SimCluster`] starts N nodes over one `SimNetwork`, generic over the
//! storage via a builder closure.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::time::Duration;

use openraft::error::{
    InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Timeout, Unreachable,
};
use openraft::network::{Backoff, RPCOption, RPCTypes, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::{RaftLogStorage, RaftStateMachine};
use openraft::{BasicNode, Raft};
use tokio::sync::Notify;

use crate::forward::{ForwardError, ForwardHandler, ForwardTransport};
use crate::status::{NodeStatus, StatusSource, StatusTransport};
use crate::wire::{RpcRequest, RpcResponse, WireError};
use crate::{ForwardRequest, ForwardResponse, NodeId, TypeConfig};

/// SplitMix64: a tiny, stable, seedable generator (the stream never changes
/// with a dependency upgrade).
#[derive(Debug, Clone)]
pub struct SimRng(u64);

impl SimRng {
    pub fn new(seed: u64) -> Self {
        SimRng(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `[0, 1)`.
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Uniform in `[lo, hi]` (`lo` if `hi <= lo`).
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        if hi <= lo {
            return lo;
        }
        let span = hi - lo;
        if span == u64::MAX {
            return self.next_u64();
        }
        lo + self.next_u64() % (span + 1)
    }
}

/// Timeouts and backoff of simulated RPCs.
#[derive(Debug, Clone)]
pub struct SimConfig {
    pub append_timeout: Duration,
    pub vote_timeout: Duration,
    pub snapshot_timeout: Duration,
    pub forward_timeout: Duration,
    /// Returned by `RaftNetwork::backoff` (constant).
    pub backoff: Duration,
}

impl Default for SimConfig {
    fn default() -> Self {
        SimConfig {
            append_timeout: Duration::from_millis(200),
            vote_timeout: Duration::from_millis(200),
            snapshot_timeout: Duration::from_secs(2),
            forward_timeout: Duration::from_millis(500),
            backoff: Duration::from_millis(50),
        }
    }
}

/// What happened to one message, as recorded in the fault log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FaultKind {
    /// The link was blocked: the request failed as `Unreachable`.
    Blocked,
    DropRequest,
    DropResponse,
    Duplicate,
    Delay(Duration),
}

/// One injected fault: message number `seq` on the link `from → to`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FaultEvent {
    pub from: NodeId,
    pub to: NodeId,
    pub seq: u64,
    pub kind: FaultKind,
}

/// The per-message decision (a pure function of seed, link and seq).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Decision {
    pub drop_request: bool,
    pub drop_response: bool,
    pub duplicate: bool,
    pub delay: Duration,
    pub dup_delay: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct Probabilities {
    drop: f64,
    dup: f64,
    delay: (Duration, Duration),
}

struct SimState {
    seed: u64,
    probs: Probabilities,
    blocked: BTreeSet<(NodeId, NodeId)>,
    paused: BTreeSet<NodeId>,
    link_seq: HashMap<(NodeId, NodeId), u64>,
    log: Vec<FaultEvent>,
}

/// Type-erased [`ForwardHandler`].
trait DynForward: Send + Sync {
    fn forward_boxed(
        &self,
        req: ForwardRequest,
    ) -> Pin<Box<dyn Future<Output = ForwardResponse> + Send + '_>>;
}

impl<H: ForwardHandler> DynForward for H {
    fn forward_boxed(
        &self,
        req: ForwardRequest,
    ) -> Pin<Box<dyn Future<Output = ForwardResponse> + Send + '_>> {
        Box::pin(self.forward(req))
    }
}

/// The forward handler of an endpoint; without one, forwards are answered
/// `NotLeader { leader: None }`.
struct EndpointHandler(Option<Arc<dyn DynForward>>);

impl ForwardHandler for EndpointHandler {
    async fn forward(&self, req: ForwardRequest) -> ForwardResponse {
        match &self.0 {
            Some(h) => h.forward_boxed(req).await,
            None => ForwardResponse::NotLeader { leader: None },
        }
    }
}

#[derive(Clone)]
struct Endpoint {
    raft: Raft<TypeConfig>,
    handler: Arc<EndpointHandler>,
}

struct SimInner {
    cfg: SimConfig,
    state: Mutex<SimState>,
    endpoints: RwLock<HashMap<NodeId, Endpoint>>,
    /// Vote gates by node (kept across re-registration and restarts).
    vote_gates: RwLock<HashMap<NodeId, Arc<crate::listener::VoteGate>>>,
    /// Status sources by node (removed by `unregister`).
    status: RwLock<HashMap<NodeId, Arc<dyn StatusSource>>>,
    /// Woken whenever a node is resumed.
    resumed: Notify,
}

/// The simulated network (see the module docs). Cheap to clone.
#[derive(Clone)]
pub struct SimNetwork {
    inner: Arc<SimInner>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Mixes a link and sequence number into the seed (SplitMix64 finalizer).
fn message_seed(seed: u64, from: NodeId, to: NodeId, seq: u64) -> u64 {
    let mut r = SimRng::new(seed ^ from.rotate_left(17) ^ to.rotate_left(41));
    let a = r.next_u64();
    SimRng::new(a ^ seq.wrapping_mul(0xD6E8_FEB8_6659_FD93)).next_u64()
}

fn decide(seed: u64, probs: Probabilities, from: NodeId, to: NodeId, seq: u64) -> Decision {
    let mut r = SimRng::new(message_seed(seed, from, to, seq));
    // Always draw the same number of values so that changing one
    // probability does not shift the others.
    let (p_req, p_resp, p_dup) = (r.next_f64(), r.next_f64(), r.next_f64());
    let lo = probs.delay.0.as_micros() as u64;
    let hi = probs.delay.1.as_micros() as u64;
    let (d1, d2) = (r.range(lo, hi), r.range(lo, hi));
    Decision {
        drop_request: p_req < probs.drop,
        drop_response: p_resp < probs.drop,
        duplicate: p_dup < probs.dup,
        delay: Duration::from_micros(d1),
        dup_delay: Duration::from_micros(d2),
    }
}

impl SimNetwork {
    pub fn new(seed: u64, cfg: SimConfig) -> Self {
        SimNetwork {
            inner: Arc::new(SimInner {
                cfg,
                state: Mutex::new(SimState {
                    seed,
                    probs: Probabilities {
                        drop: 0.0,
                        dup: 0.0,
                        delay: (Duration::ZERO, Duration::ZERO),
                    },
                    blocked: BTreeSet::new(),
                    paused: BTreeSet::new(),
                    link_seq: HashMap::new(),
                    log: Vec::new(),
                }),
                endpoints: RwLock::new(HashMap::new()),
                vote_gates: RwLock::new(HashMap::new()),
                status: RwLock::new(HashMap::new()),
                resumed: Notify::new(),
            }),
        }
    }

    pub fn seed(&self) -> u64 {
        lock(&self.inner.state).seed
    }

    /// The network as seen by node `id` (its Raft network factory and
    /// forward transport).
    pub fn node(&self, id: NodeId) -> SimNode {
        SimNode {
            id,
            net: self.clone(),
        }
    }

    /// Makes `raft` reachable as node `id`, forwards going to `handler`.
    pub fn register<H: ForwardHandler>(
        &self,
        id: NodeId,
        raft: Raft<TypeConfig>,
        handler: Option<Arc<H>>,
    ) {
        let handler = EndpointHandler(handler.map(|h| h as Arc<dyn DynForward>));
        self.inner
            .endpoints
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                id,
                Endpoint {
                    raft,
                    handler: Arc::new(handler),
                },
            );
    }

    /// Gives node `id` a vote gate, as [`crate::listener::ListenerConfig`]
    /// does on TCP (`None` removes it). It stays in place across
    /// restarts of the node until changed.
    pub fn set_vote_gate(&self, id: NodeId, gate: Option<Arc<crate::listener::VoteGate>>) {
        let mut gates = self
            .inner
            .vote_gates
            .write()
            .unwrap_or_else(|e| e.into_inner());
        match gate {
            Some(g) => gates.insert(id, g),
            None => gates.remove(&id),
        };
    }

    fn vote_gate(&self, id: NodeId) -> Option<Arc<crate::listener::VoteGate>> {
        self.inner
            .vote_gates
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&id)
            .cloned()
    }

    /// Makes node `id` unreachable (for example before restarting it),
    /// status probes included.
    pub fn unregister(&self, id: NodeId) {
        self.inner
            .endpoints
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&id);
        self.set_status_source(id, None);
    }

    /// Makes node `id` answer status probes from `source` (`None`: not at
    /// all), independently of whether its Raft is registered.
    pub fn set_status_source(&self, id: NodeId, source: Option<Arc<dyn StatusSource>>) {
        let mut st = self.inner.status.write().unwrap_or_else(|e| e.into_inner());
        match source {
            Some(s) => st.insert(id, s),
            None => st.remove(&id),
        };
    }

    fn status_source(&self, id: NodeId) -> Option<Arc<dyn StatusSource>> {
        self.inner
            .status
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&id)
            .cloned()
    }

    fn endpoint(&self, id: NodeId) -> Option<Endpoint> {
        self.inner
            .endpoints
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&id)
            .cloned()
    }

    /// Blocks messages from `from` to `to` (one direction).
    pub fn block(&self, from: NodeId, to: NodeId) {
        lock(&self.inner.state).blocked.insert((from, to));
    }

    pub fn unblock(&self, from: NodeId, to: NodeId) {
        lock(&self.inner.state).blocked.remove(&(from, to));
    }

    /// Blocks both directions between `a` and `b`.
    pub fn partition(&self, a: NodeId, b: NodeId) {
        let mut s = lock(&self.inner.state);
        s.blocked.insert((a, b));
        s.blocked.insert((b, a));
    }

    /// Cuts `id` off from every node in `others`, both directions.
    pub fn isolate(&self, id: NodeId, others: &[NodeId]) {
        for &o in others {
            if o != id {
                self.partition(id, o);
            }
        }
    }

    /// Removes every partition and resumes every paused node.
    pub fn heal(&self) {
        let mut s = lock(&self.inner.state);
        s.blocked.clear();
        s.paused.clear();
        drop(s);
        self.inner.resumed.notify_waiters();
    }

    pub fn pause(&self, id: NodeId) {
        lock(&self.inner.state).paused.insert(id);
    }

    pub fn resume(&self, id: NodeId) {
        lock(&self.inner.state).paused.remove(&id);
        self.inner.resumed.notify_waiters();
    }

    /// Per-message loss probability, for requests and responses separately.
    pub fn set_drop(&self, p: f64) {
        lock(&self.inner.state).probs.drop = p.clamp(0.0, 1.0);
    }

    pub fn set_duplicate(&self, p: f64) {
        lock(&self.inner.state).probs.dup = p.clamp(0.0, 1.0);
    }

    pub fn set_delay(&self, min: Duration, max: Duration) {
        lock(&self.inner.state).probs.delay = (min, max.max(min));
    }

    /// Faults injected so far, in the order they were decided.
    pub fn fault_log(&self) -> Vec<FaultEvent> {
        lock(&self.inner.state).log.clone()
    }

    /// Faults injected on one directed link, in link order (reproducible
    /// from the seed for a given number of messages on the link).
    pub fn link_fault_log(&self, from: NodeId, to: NodeId) -> Vec<FaultEvent> {
        let mut v: Vec<_> = lock(&self.inner.state)
            .log
            .iter()
            .filter(|e| e.from == from && e.to == to)
            .cloned()
            .collect();
        v.sort_by_key(|e| e.seq);
        v
    }

    /// Takes the decision for the next message on `from → to` and records
    /// its faults. `None` if the link is blocked.
    pub fn next_decision(&self, from: NodeId, to: NodeId) -> Option<Decision> {
        let mut s = lock(&self.inner.state);
        let seq = {
            let c = s.link_seq.entry((from, to)).or_insert(0);
            *c += 1;
            *c
        };
        if s.blocked.contains(&(from, to)) {
            s.log.push(FaultEvent {
                from,
                to,
                seq,
                kind: FaultKind::Blocked,
            });
            return None;
        }
        let d = decide(s.seed, s.probs, from, to, seq);
        let mut record = |kind| {
            s.log.push(FaultEvent {
                from,
                to,
                seq,
                kind,
            })
        };
        if d.drop_request {
            record(FaultKind::DropRequest);
        }
        if d.drop_response {
            record(FaultKind::DropResponse);
        }
        if d.duplicate {
            record(FaultKind::Duplicate);
        }
        if !d.delay.is_zero() {
            record(FaultKind::Delay(d.delay));
        }
        Some(d)
    }

    fn is_blocked(&self, from: NodeId, to: NodeId) -> bool {
        lock(&self.inner.state).blocked.contains(&(from, to))
    }

    async fn wait_unpaused(&self, a: NodeId, b: NodeId) {
        loop {
            let resumed = self.inner.resumed.notified();
            {
                let s = lock(&self.inner.state);
                if !s.paused.contains(&a) && !s.paused.contains(&b) {
                    return;
                }
            }
            resumed.await;
        }
    }

    /// Applies one action of a [`FaultSchedule`].
    pub fn apply(&self, action: &FaultAction) {
        match action {
            FaultAction::Partition(a, b) => self.partition(*a, *b),
            FaultAction::Block(a, b) => self.block(*a, *b),
            FaultAction::Isolate(id, others) => self.isolate(*id, others),
            FaultAction::Pause(id) => self.pause(*id),
            FaultAction::Resume(id) => self.resume(*id),
            FaultAction::SetDrop(p) => self.set_drop(*p),
            FaultAction::SetDuplicate(p) => self.set_duplicate(*p),
            FaultAction::SetDelay(lo, hi) => self.set_delay(*lo, *hi),
            FaultAction::Heal => self.heal(),
        }
    }

    /// One simulated request from `from` to `to`.
    async fn call(
        &self,
        from: NodeId,
        to: NodeId,
        body: RpcRequest,
        timeout: Duration,
    ) -> Result<RpcResponse, SimError> {
        let exchange = async {
            let Some(d) = self.next_decision(from, to) else {
                return Err(SimError::Unreachable(format!(
                    "link {from} -> {to} blocked"
                )));
            };
            self.wait_unpaused(from, to).await;
            tokio::time::sleep(d.delay).await;
            if d.drop_request {
                std::future::pending::<()>().await;
            }
            if matches!(body, RpcRequest::Status) {
                let Some(src) = self.status_source(to) else {
                    return Err(SimError::Unreachable(format!("node {to} is not running")));
                };
                let resp = RpcResponse::status(src.status());
                if d.drop_response || self.is_blocked(to, from) {
                    std::future::pending::<()>().await;
                }
                self.wait_unpaused(from, to).await;
                return Ok(resp);
            }
            let Some(ep) = self.endpoint(to) else {
                return Err(SimError::Unreachable(format!("node {to} is not running")));
            };
            let gate = self.vote_gate(to);
            if d.duplicate {
                let (ep2, body2, delay) = (ep.clone(), body.clone(), d.dup_delay);
                let gate2 = gate.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(delay).await;
                    let _ = crate::listener::dispatch(
                        from,
                        body2,
                        Some((&ep2.raft, &*ep2.handler)),
                        gate2.as_deref(),
                        None,
                    )
                    .await;
                });
            }
            let resp = crate::listener::dispatch(
                from,
                body,
                Some((&ep.raft, &*ep.handler)),
                gate.as_deref(),
                None,
            )
            .await;
            if d.drop_response || self.is_blocked(to, from) {
                std::future::pending::<()>().await;
            }
            self.wait_unpaused(from, to).await;
            Ok(resp)
        };
        match tokio::time::timeout(timeout, exchange).await {
            Ok(r) => r,
            Err(_) => Err(SimError::Timeout(timeout)),
        }
    }
}

#[derive(Debug)]
enum SimError {
    Unreachable(String),
    Timeout(Duration),
}

/// One node's view of the [`SimNetwork`].
#[derive(Clone)]
pub struct SimNode {
    id: NodeId,
    net: SimNetwork,
}

impl RaftNetworkFactory<TypeConfig> for SimNode {
    type Network = SimClient;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> SimClient {
        SimClient {
            from: self.id,
            to: target,
            net: self.net.clone(),
        }
    }
}

impl ForwardTransport for SimNode {
    async fn forward(
        &self,
        target: NodeId,
        req: ForwardRequest,
    ) -> Result<ForwardResponse, ForwardError> {
        let t = self.net.inner.cfg.forward_timeout;
        match self
            .net
            .call(self.id, target, RpcRequest::Forward(req), t)
            .await
        {
            Ok(RpcResponse::Forward(Ok(r))) => Ok(r),
            Ok(RpcResponse::Forward(Err(e))) => Err(ForwardError::Rejected(e.to_string())),
            Ok(_) => Err(ForwardError::Network("unexpected response kind".into())),
            Err(SimError::Unreachable(m)) => Err(ForwardError::Unreachable(m)),
            Err(SimError::Timeout(_)) => Err(ForwardError::Timeout),
        }
    }
}

impl StatusTransport for SimNode {
    async fn status(&self, target: NodeId) -> Result<NodeStatus, ForwardError> {
        let t = self.net.inner.cfg.forward_timeout;
        match self.net.call(self.id, target, RpcRequest::Status, t).await {
            Ok(r) => r
                .into_status()
                .ok_or_else(|| ForwardError::Network("unexpected response kind".into())),
            Err(SimError::Unreachable(m)) => Err(ForwardError::Unreachable(m)),
            Err(SimError::Timeout(_)) => Err(ForwardError::Timeout),
        }
    }
}

/// openraft's client from one node to another over the [`SimNetwork`].
pub struct SimClient {
    from: NodeId,
    to: NodeId,
    net: SimNetwork,
}

type RpcErr<E = openraft::error::Infallible> = RPCError<NodeId, BasicNode, RaftError<NodeId, E>>;

impl SimClient {
    fn err<E: std::error::Error>(&self, action: RPCTypes, e: SimError) -> RpcErr<E> {
        match e {
            SimError::Unreachable(m) => {
                RPCError::Unreachable(Unreachable::new(&io::Error::other(m)))
            }
            SimError::Timeout(t) => RPCError::Timeout(Timeout {
                action,
                id: self.from,
                target: self.to,
                timeout: t,
            }),
        }
    }

    fn remote<E: std::error::Error>(&self, e: WireError) -> RpcErr<E> {
        match e {
            WireError::Fatal(f) => {
                RPCError::RemoteError(RemoteError::new(self.to, RaftError::Fatal(f.into_fatal())))
            }
            other => RPCError::Network(NetworkError::new(&io::Error::other(other.to_string()))),
        }
    }
}

fn unexpected<E: std::error::Error>() -> RpcErr<E> {
    RPCError::Network(NetworkError::new(&io::Error::other(
        "unexpected response kind",
    )))
}

impl RaftNetwork<TypeConfig> for SimClient {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RpcErr> {
        let t = self.net.inner.cfg.append_timeout.min(option.hard_ttl());
        match self
            .net
            .call(self.from, self.to, RpcRequest::AppendEntries(rpc), t)
            .await
        {
            Ok(RpcResponse::AppendEntries(Ok(r))) => Ok(r),
            Ok(RpcResponse::AppendEntries(Err(e))) => Err(self.remote(e)),
            Ok(_) => Err(unexpected()),
            Err(e) => Err(self.err(RPCTypes::AppendEntries, e)),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, RpcErr<InstallSnapshotError>> {
        let t = self.net.inner.cfg.snapshot_timeout.min(option.hard_ttl());
        match self
            .net
            .call(self.from, self.to, RpcRequest::InstallSnapshot(rpc), t)
            .await
        {
            Ok(RpcResponse::InstallSnapshot(Ok(r))) => Ok(r),
            Ok(RpcResponse::InstallSnapshot(Err(WireError::SnapshotMismatch(m)))) => {
                Err(RPCError::RemoteError(RemoteError::new(
                    self.to,
                    RaftError::APIError(InstallSnapshotError::SnapshotMismatch(m)),
                )))
            }
            Ok(RpcResponse::InstallSnapshot(Err(e))) => Err(self.remote(e)),
            Ok(_) => Err(unexpected()),
            Err(e) => Err(self.err(RPCTypes::InstallSnapshot, e)),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RpcErr> {
        let t = self.net.inner.cfg.vote_timeout.min(option.hard_ttl());
        match self
            .net
            .call(self.from, self.to, RpcRequest::Vote(rpc), t)
            .await
        {
            Ok(RpcResponse::Vote(Ok(r))) => Ok(r),
            Ok(RpcResponse::Vote(Err(e))) => Err(self.remote(e)),
            Ok(_) => Err(unexpected()),
            Err(e) => Err(self.err(RPCTypes::Vote, e)),
        }
    }

    fn backoff(&self) -> Backoff {
        Backoff::new(std::iter::repeat(self.net.inner.cfg.backoff))
    }
}

/// One action of a fault schedule.
#[derive(Debug, Clone, PartialEq)]
pub enum FaultAction {
    Partition(NodeId, NodeId),
    Block(NodeId, NodeId),
    Isolate(NodeId, Vec<NodeId>),
    Pause(NodeId),
    Resume(NodeId),
    SetDrop(f64),
    SetDuplicate(f64),
    SetDelay(Duration, Duration),
    Heal,
}

/// A timed sequence of fault actions derived from a seed.
#[derive(Debug, Clone, PartialEq)]
pub struct FaultSchedule {
    pub seed: u64,
    /// `(offset from the start, action)`, in increasing offset order.
    pub steps: Vec<(Duration, FaultAction)>,
}

impl FaultSchedule {
    /// `steps` random actions over `nodes`, `gap` apart on average; the
    /// schedule ends with `Heal`.
    pub fn generate(seed: u64, nodes: &[NodeId], steps: usize, gap: Duration) -> Self {
        let mut r = SimRng::new(seed);
        let mut at = Duration::ZERO;
        let mut out = Vec::with_capacity(steps + 1);
        let gap_us = gap.as_micros() as u64;
        let pick = |r: &mut SimRng| -> NodeId {
            let i = r.range(0, nodes.len().saturating_sub(1) as u64) as usize;
            nodes.get(i).copied().unwrap_or(0)
        };
        for _ in 0..steps {
            at += Duration::from_micros(r.range(gap_us / 2, gap_us + gap_us / 2));
            let action = match r.range(0, 8) {
                0 => FaultAction::Partition(pick(&mut r), pick(&mut r)),
                1 => FaultAction::Block(pick(&mut r), pick(&mut r)),
                2 => FaultAction::Isolate(pick(&mut r), nodes.to_vec()),
                3 => FaultAction::Pause(pick(&mut r)),
                4 => FaultAction::Resume(pick(&mut r)),
                5 => FaultAction::SetDrop(r.range(0, 30) as f64 / 100.0),
                6 => FaultAction::SetDuplicate(r.range(0, 30) as f64 / 100.0),
                7 => {
                    let lo = r.range(0, 5_000);
                    FaultAction::SetDelay(
                        Duration::from_micros(lo),
                        Duration::from_micros(lo + r.range(0, 20_000)),
                    )
                }
                _ => FaultAction::Heal,
            };
            out.push((at, action));
        }
        out.push((at + gap, FaultAction::Heal));
        FaultSchedule { seed, steps: out }
    }

    /// Applies the steps to `net` at their offsets from now.
    pub async fn run(&self, net: &SimNetwork) {
        let start = tokio::time::Instant::now();
        for (at, action) in &self.steps {
            tokio::time::sleep_until(start + *at).await;
            tracing::debug!(?action, "sim fault");
            net.apply(action);
        }
    }
}

/// N openraft nodes over one [`SimNetwork`], generic over the storage.
pub struct SimCluster {
    pub net: SimNetwork,
    config: Arc<openraft::Config>,
    nodes: BTreeMap<NodeId, Raft<TypeConfig>>,
}

impl SimCluster {
    /// Starts nodes `ids` with storage from `build(id)`. Forwards to these
    /// nodes are answered `NotLeader` until [`SimCluster::set_forward_handler`].
    pub async fn start<F, Fut, LS, SM>(
        ids: &[NodeId],
        config: openraft::Config,
        net: SimNetwork,
        mut build: F,
    ) -> Result<Self, String>
    where
        F: FnMut(NodeId) -> Fut,
        Fut: Future<Output = (LS, SM)>,
        LS: RaftLogStorage<TypeConfig>,
        SM: RaftStateMachine<TypeConfig>,
    {
        let config = Arc::new(config.validate().map_err(|e| e.to_string())?);
        let mut cluster = SimCluster {
            net,
            config,
            nodes: BTreeMap::new(),
        };
        for &id in ids {
            let (log, sm) = build(id).await;
            cluster.start_node(id, log, sm).await?;
        }
        Ok(cluster)
    }

    /// Starts (or restarts, after [`SimCluster::stop_node`]) node `id`.
    pub async fn start_node<LS, SM>(&mut self, id: NodeId, log: LS, sm: SM) -> Result<(), String>
    where
        LS: RaftLogStorage<TypeConfig>,
        SM: RaftStateMachine<TypeConfig>,
    {
        let raft = Raft::new(id, self.config.clone(), self.net.node(id), log, sm)
            .await
            .map_err(|e| format!("node {id}: {e}"))?;
        self.net.register::<EndpointHandler>(id, raft.clone(), None);
        self.nodes.insert(id, raft);
        Ok(())
    }

    /// Shuts node `id` down and makes it unreachable.
    pub async fn stop_node(&mut self, id: NodeId) {
        self.net.unregister(id);
        if let Some(r) = self.nodes.remove(&id) {
            let _ = r.shutdown().await;
        }
    }

    pub fn set_forward_handler<H: ForwardHandler>(&self, id: NodeId, handler: Arc<H>) {
        if let Some(r) = self.nodes.get(&id) {
            self.net.register(id, r.clone(), Some(handler));
        }
    }

    pub fn ids(&self) -> Vec<NodeId> {
        self.nodes.keys().copied().collect()
    }

    pub fn raft(&self, id: NodeId) -> Option<&Raft<TypeConfig>> {
        self.nodes.get(&id)
    }

    /// Initializes membership (every started node a voter) through the
    /// lowest node id.
    pub async fn initialize(&self) -> Result<(), String> {
        let members: BTreeMap<NodeId, BasicNode> = self
            .nodes
            .keys()
            .map(|&id| (id, BasicNode::new(format!("sim-{id}"))))
            .collect();
        let Some(first) = self.nodes.values().next() else {
            return Err("no nodes".into());
        };
        first.initialize(members).await.map_err(|e| e.to_string())
    }

    /// Waits until every node in `among` sees the same leader, itself in
    /// `among` and in the leader state.
    pub async fn wait_for_leader(
        &self,
        among: &[NodeId],
        timeout: Duration,
    ) -> Result<NodeId, String> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(l) = self.agreed_leader(among) {
                return Ok(l);
            }
            if tokio::time::Instant::now() >= deadline {
                let seen: Vec<_> = among
                    .iter()
                    .filter_map(|id| {
                        let m = self.nodes.get(id)?.metrics().borrow().clone();
                        Some((*id, m.current_leader, m.current_term, m.state))
                    })
                    .collect();
                return Err(format!("no agreed leader among {among:?}: {seen:?}"));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn agreed_leader(&self, among: &[NodeId]) -> Option<NodeId> {
        let mut leader = None;
        for id in among {
            let m = self.nodes.get(id)?.metrics().borrow().clone();
            let l = m.current_leader?;
            if leader.is_some_and(|x| x != l) {
                return None;
            }
            leader = Some(l);
        }
        let l = leader?;
        if !among.contains(&l) {
            return None;
        }
        let lm = self.nodes.get(&l)?.metrics().borrow().clone();
        lm.state.is_leader().then_some(l)
    }

    /// Waits until every node in `among` has applied at least `index`.
    pub async fn wait_applied(
        &self,
        among: &[NodeId],
        index: u64,
        timeout: Duration,
    ) -> Result<(), String> {
        for id in among {
            let Some(r) = self.nodes.get(id) else {
                return Err(format!("node {id} is not running"));
            };
            r.wait(Some(timeout))
                .applied_index_at_least(Some(index), "sim wait_applied")
                .await
                .map_err(|e| format!("node {id}: {e}"))?;
        }
        Ok(())
    }

    /// Shuts every node down.
    pub async fn shutdown(self) {
        for (_, r) in self.nodes {
            let _ = r.shutdown().await;
        }
    }
}
