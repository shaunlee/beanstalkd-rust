//! In-process chaos harness: N openraft nodes with the real storage
//! (`bstk_raft::storage::open` on temporary directories, the real
//! `ClusterStateMachine`) over the simulated network (`SimNetwork`), driven
//! by a minimal emulation of the server's owner logic, under a seeded
//! schedule of faults.
//!
//! # Time
//!
//! Each seed runs on its own current-thread tokio runtime with paused
//! time: timers (openraft's, the network's, the workload's) auto-advance
//! when every task is idle, so a run of tens of virtual seconds takes a
//! fraction of that in wall time. Storage I/O is synchronous, so it takes
//! no virtual time. A node's clock is `ANCHOR + virtual elapsed + skew`;
//! history times are virtual elapsed, so the checker's `slack` is the
//! largest absolute skew injected.
//!
//! Replaying a seed reproduces the fault schedule, the workload choices and
//! the network's per-message decisions; openraft's randomized election
//! timeouts are not seeded, so interleavings can differ.
//!
//! # Owner emulation (test code, deliberately minimal)
//!
//! Per node incarnation: connection ids above `highest_local + GAP`, one
//! ordered queue of `(conn, seq, input)` (`Connect` at seq 1, a put as
//! `PutStarted` then `Command::Put`, `Disconnect` on close); the leader
//! proposes queued items with `client_write_ff`, a follower forwards them
//! in batches to the leader over the simulated network (one batch in
//! flight); an item leaves the queue when the state machine reports it
//! applied; everything unapplied is resent after a leader or term change,
//! a failed forward, or a stall. The leader stamps `now = max(clock, last
//! applied now, last stamped)` and proposes `Tick` at `next_deadline()`.
//! A (re)started node waits until it has caught up, has `DropNode(self,
//! highest_local)` committed through the current leader, and only then
//! accepts connections.
//!
//! # Faults
//!
//! Partitions (pairs, one-way blocks, isolation of a node, minority /
//! majority splits), message loss, delay and duplication, paused nodes,
//! clock skew per node, crash (all `Raft` handles and the storage dropped
//! without `shutdown`) and restart from the same directory, crash of the
//! current leader, crash of every node, and a wiped node rejoining (its
//! directory emptied; it catches up through a snapshot install).
//!
//! # Checks
//!
//! - every applied log index has the same entry on every node and
//!   incarnation (committed entries are never lost or changed, also across
//!   crashes and restarts);
//! - every replica's engine state is identical at the same applied index
//!   (hash of `export_state` after each applied batch and each snapshot
//!   install);
//! - the replies each connection received are a prefix of what a
//!   single-engine replay of the committed log sends it;
//! - the history checker on the replies the clients received;
//! - liveness: after healing, a leader serves, every node accepts
//!   connections and the final verification completes.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use bstk_engine::{ConnId, EngineConfig, EngineInput, Nanos, StaticSysInfo};
use bstk_proto::{Command, Response};
use bstk_raft::forward::{ForwardError, ForwardHandler, ForwardTransport};
use bstk_raft::sim::{FaultAction, SimConfig, SimNetwork, SimRng};
use bstk_raft::storage::{
    self, ClusterStateMachine, LogOptions, OpenError, ReplySink, SmOptions, StateHandle,
};
use bstk_raft::{
    Applied, CONN_SEQ_BITS, ForwardRequest, ForwardResponse, NodeId, Op, Request, TypeConfig,
    conn_id, owner_of,
};
use openraft::storage::RaftStateMachine;
use openraft::{
    BasicNode, Entry, LogId, Raft, ServerState, Snapshot, SnapshotMeta, SnapshotPolicy,
    StorageError, StoredMembership,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::checker::{self, CheckConfig, Report};
use crate::history::{Cmd, ConnKey, History, JobId, Recorder, Reply};
use crate::workload::{ClientState, Known, WorkloadConfig};

/// Engine time at the start of a run (any wall-like value).
const ANCHOR: Nanos = 1_700_000_000_000_000_000;
/// New connections of an incarnation start this far above the highest
/// local number the state has seen for the node (as the server does).
const GAP: u64 = 1 << 20;
/// Resend everything unapplied if the front item waits this long.
const STALL: Duration = Duration::from_secs(1);

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// One run's parameters (all derived from the seed by [`RunConfig::from_seed`]).
#[derive(Debug, Clone)]
pub struct RunConfig {
    pub seed: u64,
    pub nodes: u64,
    pub clients: usize,
    /// Virtual duration of the workload under faults.
    pub duration: Duration,
    pub fault_steps: usize,
    /// Largest absolute clock skew injected (ms).
    pub max_skew_ms: u64,
    /// Include wiped-node rejoins in the schedule. Off by default: a node
    /// rejoining with an empty directory under the same id is not safe in
    /// Raft (it forgets its vote and the entries it acknowledged), and
    /// openraft 0.9 panics on the leader when it happens (see the report
    /// and `tests/findings.rs`).
    pub wipe: bool,
    pub work: WorkloadConfig,
}

impl RunConfig {
    pub fn from_seed(seed: u64) -> RunConfig {
        let mut r = SimRng::new(seed ^ 0x5EED_C0DE);
        RunConfig {
            seed,
            nodes: if seed % 4 == 3 { 5 } else { 3 },
            clients: r.range(3, 6) as usize,
            duration: Duration::from_secs(r.range(8, 16)),
            fault_steps: r.range(6, 14) as usize,
            max_skew_ms: if r.range(0, 2) == 0 {
                0
            } else {
                r.range(50, 400)
            },
            wipe: std::env::var("BSTK_CHAOS_WIPE").is_ok(),
            work: WorkloadConfig::default(),
        }
    }
}

/// A fault of the in-process schedule.
#[derive(Debug, Clone, PartialEq)]
pub enum Fault {
    Net(FaultAction),
    /// Cut the network into these two sides (both directions).
    Split(Vec<NodeId>, Vec<NodeId>),
    Crash(NodeId),
    CrashLeader,
    CrashAll,
    Restart(NodeId),
    RestartAll,
    /// Crash, empty the data directory, restart.
    Wipe(NodeId),
    /// Clock offset of a node (ms, may be negative).
    Skew(NodeId, i64),
}

/// Generates `steps` faults `gap` apart on average (ends healed and with
/// every node running, but that is also enforced after the schedule).
pub fn generate_faults(
    seed: u64,
    nodes: &[NodeId],
    steps: usize,
    gap: Duration,
    max_skew_ms: u64,
    wipe: bool,
) -> Vec<(Duration, Fault)> {
    let mut r = SimRng::new(seed ^ 0xFA17);
    let mut at = Duration::ZERO;
    let gap_ms = gap.as_millis() as u64;
    let n = nodes.len() as u64;
    let pick = |r: &mut SimRng| nodes[r.range(0, n - 1) as usize];
    let mut out = Vec::new();
    for _ in 0..steps {
        at += Duration::from_millis(r.range(gap_ms / 2, gap_ms + gap_ms / 2));
        let f = match r.range(0, 19) {
            0 => Fault::Net(FaultAction::Partition(pick(&mut r), pick(&mut r))),
            1 => Fault::Net(FaultAction::Block(pick(&mut r), pick(&mut r))),
            2 | 3 => Fault::Net(FaultAction::Isolate(pick(&mut r), nodes.to_vec())),
            4 => {
                // A random split: a minority on one side.
                let mut v = nodes.to_vec();
                for i in (1..v.len()).rev() {
                    let j = r.range(0, i as u64) as usize;
                    v.swap(i, j);
                }
                let k = r.range(1, (n - 1) / 2) as usize;
                let (a, b) = v.split_at(k);
                Fault::Split(a.to_vec(), b.to_vec())
            }
            5 => Fault::Net(FaultAction::Pause(pick(&mut r))),
            6 => Fault::Net(FaultAction::Resume(pick(&mut r))),
            7 => Fault::Net(FaultAction::SetDrop(r.range(0, 25) as f64 / 100.0)),
            8 => Fault::Net(FaultAction::SetDuplicate(r.range(0, 30) as f64 / 100.0)),
            9 => {
                let lo = r.range(0, 5);
                Fault::Net(FaultAction::SetDelay(
                    Duration::from_millis(lo),
                    Duration::from_millis(lo + r.range(0, 40)),
                ))
            }
            10 | 11 => Fault::Net(FaultAction::Heal),
            12 => Fault::Crash(pick(&mut r)),
            13 => Fault::CrashLeader,
            14 => Fault::Restart(pick(&mut r)),
            15 => Fault::RestartAll,
            16 if wipe => Fault::Wipe(pick(&mut r)),
            16 => Fault::Restart(pick(&mut r)),
            17 => {
                if r.range(0, 3) == 0 {
                    Fault::CrashAll
                } else {
                    Fault::CrashLeader
                }
            }
            _ => {
                let s = if max_skew_ms == 0 {
                    0
                } else {
                    r.range(0, 2 * max_skew_ms) as i64 - max_skew_ms as i64
                };
                Fault::Skew(pick(&mut r), s)
            }
        };
        out.push((at, f));
    }
    out
}

// ---------------------------------------------------------------------------
// Ledger: what every node applied
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Ledger {
    /// Index → (serialized entry, first node that applied it).
    entries: BTreeMap<u64, (Vec<u8>, NodeId)>,
    /// Applied index → (engine state hash, first node).
    states: BTreeMap<u64, (u64, NodeId)>,
    problems: Vec<String>,
    snapshot_installs: u64,
}

impl Ledger {
    fn entry(&mut self, node: NodeId, index: u64, bytes: Vec<u8>) {
        match self.entries.get(&index) {
            None => {
                self.entries.insert(index, (bytes, node));
            }
            Some((b, first)) if *b != bytes => {
                let (first, len) = (*first, b.len());
                self.problems.push(format!(
                    "log divergence: node {node} applied a different entry at index {index} \
                     than node {first} ({} vs {len} bytes)",
                    bytes.len()
                ));
            }
            Some(_) => {}
        }
    }

    fn state(&mut self, node: NodeId, index: u64, hash: u64, what: &str) {
        match self.states.get(&index) {
            None => {
                self.states.insert(index, (hash, node));
            }
            Some((h, first)) if *h != hash => {
                let first = *first;
                self.problems.push(format!(
                    "state divergence at applied index {index}: node {node} ({what}) differs \
                     from node {first}"
                ));
            }
            Some(_) => {}
        }
    }
}

fn state_hash(h: &StateHandle) -> Option<u64> {
    let st = h.export_state()?;
    let bytes = postcard::to_allocvec(&st).ok()?;
    let mut s = DefaultHasher::new();
    bytes.hash(&mut s);
    Some(s.finish())
}

/// `ClusterStateMachine` plus recording into the [`Ledger`].
struct RecSm {
    inner: ClusterStateMachine,
    handle: StateHandle,
    node: NodeId,
    ledger: Arc<Mutex<Ledger>>,
}

type SResult<T> = Result<T, StorageError<NodeId>>;

impl RaftStateMachine<TypeConfig> for RecSm {
    type SnapshotBuilder = ClusterStateMachine;

    async fn applied_state(
        &mut self,
    ) -> SResult<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>)> {
        self.inner.applied_state().await
    }

    async fn apply<I>(&mut self, entries: I) -> SResult<Vec<Applied>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let v: Vec<Entry<TypeConfig>> = entries.into_iter().collect();
        let recs: Vec<(u64, Vec<u8>)> = v
            .iter()
            .map(|e| (e.log_id.index, postcard::to_allocvec(e).unwrap_or_default()))
            .collect();
        let res = self.inner.apply(v).await?;
        let mut l = lock(&self.ledger);
        let last = recs.last().map(|r| r.0);
        for (i, b) in recs {
            l.entry(self.node, i, b);
        }
        if let (Some(i), Some(h)) = (last, state_hash(&self.handle)) {
            l.state(self.node, i, h, "apply");
        }
        Ok(res)
    }

    async fn get_snapshot_builder(&mut self) -> ClusterStateMachine {
        self.inner.get_snapshot_builder().await
    }

    async fn begin_receiving_snapshot(&mut self) -> SResult<Box<bstk_raft::SnapshotBuf>> {
        self.inner.begin_receiving_snapshot().await
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<bstk_raft::SnapshotBuf>,
    ) -> SResult<()> {
        self.inner.install_snapshot(meta, snapshot).await?;
        let mut l = lock(&self.ledger);
        l.snapshot_installs += 1;
        if let (Some(id), Some(h)) = (meta.last_log_id, state_hash(&self.handle)) {
            l.state(self.node, id.index, h, "snapshot install");
        }
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> SResult<Option<Snapshot<TypeConfig>>> {
        self.inner.get_current_snapshot().await
    }
}

// ---------------------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------------------

type ReplyTx = mpsc::UnboundedSender<Response>;

/// The reply sink of one node incarnation.
struct Sink {
    clients: Arc<Mutex<HashMap<ConnId, ReplyTx>>>,
    applied: mpsc::UnboundedSender<(ConnId, u64)>,
    delivered: Arc<Mutex<BTreeMap<ConnId, Vec<Response>>>>,
}

impl ReplySink for Sink {
    fn applied(&self, conn: ConnId, seq: u64) {
        let _ = self.applied.send((conn, seq));
    }

    fn deliver(&self, conn: ConnId, resp: Response) {
        let clients = lock(&self.clients);
        if let Some(tx) = clients.get(&conn) {
            lock(&self.delivered)
                .entry(conn)
                .or_default()
                .push(resp.clone());
            let _ = tx.send(resp);
        }
    }

    fn closed(&self, conn: ConnId) {
        lock(&self.clients).remove(&conn);
    }
}

enum OwnerMsg {
    Input(ConnId, EngineInput),
}

/// One running incarnation of a node.
struct NodeInc {
    id: NodeId,
    raft: Raft<TypeConfig>,
    state: StateHandle,
    clients: Arc<Mutex<HashMap<ConnId, ReplyTx>>>,
    owner_tx: mpsc::UnboundedSender<OwnerMsg>,
    accepting: AtomicBool,
    next_local: AtomicU64,
    stamped: AtomicU64,
    queue_len: AtomicU64,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    run: Arc<RunShared>,
}

impl NodeInc {
    fn is_leader(&self) -> bool {
        let m = self.raft.metrics().borrow().clone();
        m.state == ServerState::Leader && m.current_leader == Some(self.id)
    }

    fn stamp(&self) -> Nanos {
        let now = self.run.clock(self.id).max(self.state.last_now());
        self.stamped.fetch_max(now, Ordering::AcqRel).max(now)
    }

    async fn propose(&self, op: Op) -> bool {
        let req = Request {
            now: self.stamp(),
            op,
        };
        self.raft.client_write_ff(req).await.is_ok()
    }
}

/// Leader side of forwarding.
struct Fwd(Arc<NodeInc>);

impl ForwardHandler for Fwd {
    async fn forward(&self, req: ForwardRequest) -> ForwardResponse {
        let inc = &self.0;
        if !inc.is_leader() {
            let leader = inc.raft.metrics().borrow().current_leader;
            return ForwardResponse::NotLeader { leader };
        }
        for (_, seq, input) in req.items {
            if !inc.propose(Op::Conn { seq, input }).await {
                return ForwardResponse::NotLeader { leader: None };
            }
        }
        ForwardResponse::Accepted
    }
}

struct Item {
    conn: ConnId,
    seq: u64,
    input: EngineInput,
}

fn local_of(conn: ConnId) -> u64 {
    conn & ((1 << CONN_SEQ_BITS) - 1)
}

/// The owner emulation of one incarnation (see the module docs).
async fn owner(
    inc: Arc<NodeInc>,
    mut rx: mpsc::UnboundedReceiver<OwnerMsg>,
    mut applied_rx: mpsc::UnboundedReceiver<(ConnId, u64)>,
) {
    let net = inc.run.net.node(inc.id);
    let mut metrics = inc.raft.metrics();
    let view_of = |m: &openraft::RaftMetrics<NodeId, BasicNode>| (m.current_leader, m.current_term);
    let mut view = view_of(&metrics.borrow());
    let mut seqs: HashMap<ConnId, u64> = HashMap::new();
    let mut queue: VecDeque<Item> = VecDeque::new();
    let mut cursor = 0usize;
    let mut front_since = Instant::now();
    let mut retry_at: Option<Instant> = None;
    let (fwd_tx, mut fwd_rx) = mpsc::unbounded_channel();
    let mut epoch = 0u64;
    let mut inflight = false;
    let mut tick = tokio::time::interval(Duration::from_millis(20));
    let mut last_tick: Option<(Nanos, Instant)> = None;
    loop {
        tokio::select! {
            m = rx.recv() => {
                let Some(OwnerMsg::Input(conn, input)) = m else { return };
                let seq = match input {
                    EngineInput::Connect(_) => {
                        seqs.insert(conn, 2);
                        Some(1)
                    }
                    EngineInput::Disconnect(_) => seqs.remove(&conn),
                    _ => seqs.get_mut(&conn).map(|s| {
                        *s += 1;
                        *s - 1
                    }),
                };
                if let Some(seq) = seq {
                    if queue.is_empty() {
                        front_since = Instant::now();
                    }
                    queue.push_back(Item { conn, seq, input });
                }
            }
            Some((conn, seq)) = applied_rx.recv() => {
                if let Some(pos) = queue.iter().position(|i| i.conn == conn && i.seq == seq) {
                    queue.remove(pos);
                    if pos < cursor {
                        cursor -= 1;
                    }
                    if pos == 0 {
                        front_since = Instant::now();
                    }
                }
            }
            r = metrics.changed() => {
                if r.is_err() {
                    return;
                }
                let v = view_of(&metrics.borrow());
                if v != view {
                    view = v;
                    cursor = 0;
                    front_since = Instant::now();
                    retry_at = None;
                    epoch += 1;
                    inflight = false;
                }
            }
            Some((e, res)) = fwd_rx.recv() => {
                if e == epoch {
                    inflight = false;
                    if !matches!(res, Ok(ForwardResponse::Accepted)) {
                        cursor = 0;
                        retry_at = Some(Instant::now() + Duration::from_millis(50));
                    }
                }
            }
            _ = tick.tick() => {
                if cursor > 0 && front_since.elapsed() >= STALL {
                    // Prune what can no longer apply, then resend.
                    let st = &inc.state;
                    let highest = st.highest_local(inc.id);
                    queue.retain(|i| {
                        local_of(i.conn) > highest
                            || st.applied_seq(i.conn).is_some_and(|a| a < i.seq)
                    });
                    cursor = 0;
                    front_since = Instant::now();
                    epoch += 1;
                    inflight = false;
                }
                // Leader duty: Tick at the next deadline.
                if inc.is_leader()
                    && let Some(d) = inc.state.next_deadline()
                    && inc.run.clock(inc.id) >= d
                    && last_tick.is_none_or(|(ld, at)| ld != d || at.elapsed() >= Duration::from_millis(300))
                {
                    if !inc.propose(Op::Tick).await {
                        return;
                    }
                    last_tick = Some((d, Instant::now()));
                }
            }
        }
        inc.queue_len.store(queue.len() as u64, Ordering::Relaxed);
        if inflight || cursor >= queue.len() || retry_at.is_some_and(|t| Instant::now() < t) {
            continue;
        }
        retry_at = None;
        if cursor == 0 {
            front_since = Instant::now();
        }
        if inc.is_leader() {
            while let Some(item) = queue.get(cursor) {
                let op = Op::Conn {
                    seq: item.seq,
                    input: item.input.clone(),
                };
                if !inc.propose(op).await {
                    return;
                }
                cursor += 1;
            }
            continue;
        }
        let Some(target) = view.0.filter(|&l| l != inc.id) else {
            continue;
        };
        let mut items = Vec::new();
        while let Some(item) = queue.get(cursor) {
            if items.len() >= 64 {
                break;
            }
            items.push((item.conn, item.seq, item.input.clone()));
            cursor += 1;
        }
        let req = ForwardRequest {
            from: inc.id,
            items,
        };
        let (net, tx, e) = (net.clone(), fwd_tx.clone(), epoch);
        inflight = true;
        tokio::spawn(async move {
            let res: Result<ForwardResponse, ForwardError> = net.forward(target, req).await;
            let _ = tx.send((e, res));
        });
    }
}

// ---------------------------------------------------------------------------
// The run
// ---------------------------------------------------------------------------

struct RunShared {
    net: SimNetwork,
    raft_cfg: Arc<openraft::Config>,
    t0: Instant,
    ids: Vec<NodeId>,
    skew: BTreeMap<NodeId, AtomicI64>,
    ledger: Arc<Mutex<Ledger>>,
    delivered: Arc<Mutex<BTreeMap<ConnId, Vec<Response>>>>,
    dirs: BTreeMap<NodeId, PathBuf>,
    nodes: Mutex<BTreeMap<NodeId, Arc<NodeInc>>>,
    problems: Mutex<Vec<String>>,
    events: Mutex<Vec<String>>,
    faults: Mutex<BTreeMap<String, u64>>,
    /// Every connection id handed out (ids must never be reused).
    used_conns: Mutex<BTreeSet<ConnId>>,
    /// History keys of client connections.
    next_key: AtomicU64,
}

impl RunShared {
    fn elapsed(&self) -> Duration {
        self.t0.elapsed()
    }

    /// Node `id`'s clock (engine nanoseconds).
    fn clock(&self, id: NodeId) -> Nanos {
        let base = ANCHOR.saturating_add(self.elapsed().as_nanos() as u64);
        let skew = self.skew.get(&id).map_or(0, |s| s.load(Ordering::Relaxed));
        base.saturating_add_signed(skew)
    }

    fn node(&self, id: NodeId) -> Option<Arc<NodeInc>> {
        lock(&self.nodes).get(&id).cloned()
    }

    fn running(&self) -> Vec<NodeId> {
        lock(&self.nodes).keys().copied().collect()
    }

    fn leader(&self) -> Option<Arc<NodeInc>> {
        lock(&self.nodes).values().find(|n| n.is_leader()).cloned()
    }

    fn problem(&self, p: String) {
        lock(&self.problems).push(p);
    }

    fn event(&self, e: String) {
        let t = self.elapsed();
        lock(&self.events).push(format!("{t:?} {e}"));
    }

    /// Crashes node `id`: drops every handle without `shutdown`.
    fn crash(&self, id: NodeId) {
        let Some(inc) = lock(&self.nodes).remove(&id) else {
            return;
        };
        self.event(format!("crash node {id}"));
        inc.accepting.store(false, Ordering::Release);
        self.net.unregister(id);
        for t in lock(&inc.tasks).drain(..) {
            t.abort();
        }
        lock(&inc.clients).clear();
    }

    /// Opens node `id`'s storage, waiting for a crashed incarnation to
    /// release its locks.
    async fn open_storage(
        &self,
        id: NodeId,
        sink: Arc<Sink>,
    ) -> Result<(storage::LogStore, ClusterStateMachine), String> {
        let dir = self.dirs.get(&id).ok_or("no data dir")?;
        for _ in 0..500 {
            let opts = SmOptions {
                node_id: id,
                engine: EngineConfig::default(),
                sys: Arc::new(|| Box::new(StaticSysInfo::default())),
                sink: sink.clone(),
            };
            match storage::open(dir, LogOptions::default(), opts) {
                Ok(s) => return Ok(s),
                Err(OpenError::Locked(_)) => tokio::time::sleep(Duration::from_millis(10)).await,
                Err(e) => return Err(format!("node {id}: open storage: {e}")),
            }
        }
        Err(format!(
            "node {id}: the crashed incarnation never released its storage lock"
        ))
    }

    /// Starts node `id` (fresh, restarted or wiped).
    async fn start(self: &Arc<Self>, id: NodeId, wipe: bool) -> Result<(), String> {
        if lock(&self.nodes).contains_key(&id) {
            return Ok(());
        }
        let clients: Arc<Mutex<HashMap<ConnId, ReplyTx>>> = Arc::default();
        let (applied_tx, applied_rx) = mpsc::unbounded_channel();
        let sink = Arc::new(Sink {
            clients: clients.clone(),
            applied: applied_tx,
            delivered: self.delivered.clone(),
        });
        if wipe {
            // Wait for the lock, then empty the directory.
            let s = self.open_storage(id, sink.clone()).await?;
            drop(s);
            let dir = self.dirs.get(&id).ok_or("no data dir")?;
            wipe_dir(dir).map_err(|e| format!("wipe node {id}: {e}"))?;
            self.event(format!("wiped node {id}"));
        }
        let (log, sm) = self.open_storage(id, sink).await?;
        let handle = sm.handle();
        let rec = RecSm {
            inner: sm,
            handle: handle.clone(),
            node: id,
            ledger: self.ledger.clone(),
        };
        let raft = Raft::new(id, self.raft_cfg.clone(), self.net.node(id), log, rec)
            .await
            .map_err(|e| format!("node {id}: raft: {e}"))?;
        let (owner_tx, owner_rx) = mpsc::unbounded_channel();
        let inc = Arc::new(NodeInc {
            id,
            raft: raft.clone(),
            state: handle,
            clients,
            owner_tx,
            accepting: AtomicBool::new(false),
            next_local: AtomicU64::new(0),
            stamped: AtomicU64::new(0),
            queue_len: AtomicU64::new(0),
            tasks: Mutex::new(Vec::new()),
            run: self.clone(),
        });
        self.net
            .register(id, raft, Some(Arc::new(Fwd(inc.clone()))));
        let owner_task = tokio::spawn(owner(inc.clone(), owner_rx, applied_rx));
        let startup = tokio::spawn(startup(inc.clone()));
        lock(&inc.tasks).extend([owner_task, startup]);
        lock(&self.nodes).insert(id, inc);
        self.event(format!("start node {id}"));
        Ok(())
    }
}

fn wipe_dir(dir: &Path) -> std::io::Result<()> {
    for ent in std::fs::read_dir(dir)? {
        let p = ent?.path();
        if p.is_dir() {
            std::fs::remove_dir_all(&p)?;
        } else {
            std::fs::remove_file(&p)?;
        }
    }
    Ok(())
}

/// A (re)started node: catch up, close out the previous incarnation's
/// connections, then accept clients.
async fn startup(inc: Arc<NodeInc>) {
    let id = inc.id;
    loop {
        // Caught up: a leader is known and everything known committed is
        // applied.
        let leader_known = inc.raft.metrics().borrow().current_leader.is_some();
        let committed = inc
            .raft
            .with_raft_state(|st| st.committed.map(|c| c.index))
            .await
            .unwrap_or(None);
        let applied = inc.state.last_applied().map(|l| l.index);
        let caught_up = leader_known && committed.is_none_or(|c| applied.is_some_and(|a| a >= c));
        if caught_up && let Some(leader) = inc.run.leader() {
            let op = Op::DropNode {
                node: id,
                up_to_local: inc.state.highest_local(id),
            };
            let req = Request {
                now: leader.stamp(),
                op,
            };
            let res =
                tokio::time::timeout(Duration::from_secs(1), leader.raft.client_write(req)).await;
            if let Ok(Ok(resp)) = res {
                let want = resp.log_id.index;
                let mut rx = inc.state.subscribe();
                let done = tokio::time::timeout(Duration::from_secs(5), async {
                    while inc.state.last_applied().is_none_or(|l| l.index < want) {
                        if rx.changed().await.is_err() {
                            return;
                        }
                    }
                })
                .await;
                if done.is_ok() && inc.state.last_applied().is_some_and(|l| l.index >= want) {
                    let first = inc.state.highest_local(id) + GAP + 1;
                    inc.next_local.store(first, Ordering::Release);
                    inc.accepting.store(true, Ordering::Release);
                    return;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A client connection to a node.
struct SimConn {
    conn: ConnId,
    rx: mpsc::UnboundedReceiver<Response>,
    owner_tx: mpsc::UnboundedSender<OwnerMsg>,
    clients: Arc<Mutex<HashMap<ConnId, ReplyTx>>>,
}

impl SimConn {
    fn connect(inc: &NodeInc) -> Option<SimConn> {
        if !inc.accepting.load(Ordering::Acquire) {
            return None;
        }
        let local = inc.next_local.fetch_add(1, Ordering::AcqRel);
        let conn = conn_id(inc.id, local);
        if !lock(&inc.run.used_conns).insert(conn) {
            inc.run.problem(format!(
                "connection id {conn} (node {}, local {local}) reused by a new incarnation of \
                 the node",
                inc.id
            ));
        }
        let (tx, rx) = mpsc::unbounded_channel();
        lock(&inc.clients).insert(conn, tx);
        inc.owner_tx
            .send(OwnerMsg::Input(conn, EngineInput::Connect(conn)))
            .ok()?;
        Some(SimConn {
            conn,
            rx,
            owner_tx: inc.owner_tx.clone(),
            clients: inc.clients.clone(),
        })
    }

    fn send(&self, input: EngineInput) -> bool {
        self.owner_tx
            .send(OwnerMsg::Input(self.conn, input))
            .is_ok()
    }

    /// Sends `cmd` and waits for its reply (`None`: none within `timeout`,
    /// or the connection was closed).
    async fn call(&mut self, cmd: &Cmd, timeout: Duration) -> Option<Response> {
        let conn = self.conn;
        let ok = match to_command(cmd) {
            Command::Put { .. } => {
                self.send(EngineInput::PutStarted {
                    conn,
                    too_big: false,
                }) && self.send(EngineInput::Command {
                    conn,
                    cmd: to_command(cmd),
                })
            }
            c => self.send(EngineInput::Command { conn, cmd: c }),
        };
        if !ok {
            return None;
        }
        tokio::time::timeout(timeout, self.rx.recv())
            .await
            .ok()
            .flatten()
    }

    fn close(self) {
        lock(&self.clients).remove(&self.conn);
        let _ = self.owner_tx.send(OwnerMsg::Input(
            self.conn,
            EngineInput::Disconnect(self.conn),
        ));
    }
}

pub fn to_command(cmd: &Cmd) -> Command {
    match cmd.clone() {
        Cmd::Put {
            pri,
            delay,
            ttr,
            body,
        } => Command::Put {
            pri,
            delay,
            ttr,
            body: body.into(),
        },
        Cmd::Reserve => Command::Reserve,
        Cmd::ReserveWithTimeout(t) => Command::ReserveWithTimeout(t),
        Cmd::Delete(id) => Command::Delete(id),
        Cmd::Release { id, pri, delay } => Command::Release { id, pri, delay },
        Cmd::Bury { id, pri } => Command::Bury { id, pri },
        Cmd::Touch(id) => Command::Touch(id),
        Cmd::Kick(n) => Command::Kick(n),
        Cmd::KickJob(id) => Command::KickJob(id),
        Cmd::Peek(id) => Command::Peek(id),
        Cmd::StatsJob(id) => Command::StatsJob(id),
    }
}

pub fn to_reply(cmd: &Cmd, r: Response) -> Reply {
    match r {
        Response::Inserted(id) => Reply::Inserted(id),
        Response::BuriedId(id) => Reply::BuriedId(id),
        Response::Buried => Reply::Buried,
        Response::Reserved { id, body } => Reply::Reserved {
            id,
            body: body.to_vec(),
        },
        Response::Found { id, body } => Reply::Found {
            id,
            body: body.to_vec(),
        },
        Response::Kicked(n) => Reply::Kicked(n),
        Response::KickedJob => Reply::KickedJob,
        Response::DeadlineSoon => Reply::DeadlineSoon,
        Response::TimedOut => Reply::TimedOut,
        Response::Deleted => Reply::Deleted,
        Response::Released => Reply::Released,
        Response::Touched => Reply::Touched,
        Response::NotFound => Reply::NotFound,
        Response::Ok(y) if matches!(cmd, Cmd::StatsJob(_)) => Reply::from_stats_yaml(&y),
        other => Reply::Other(format!("{other:?}")),
    }
}

/// Op timeout: longer than any reserve timeout.
fn op_timeout(cmd: &Cmd) -> Duration {
    match cmd {
        Cmd::ReserveWithTimeout(t) => Duration::from_secs(u64::from(*t) + 5),
        _ => Duration::from_secs(5),
    }
}

/// One workload client: connects to random nodes until `until`.
async fn client(
    run: Arc<RunShared>,
    rec: Recorder,
    known: Arc<Mutex<Known>>,
    work: WorkloadConfig,
    seed: u64,
    idx: u64,
    until: Instant,
) {
    let mut r = SimRng::new(seed ^ (idx + 1).wrapping_mul(0x9E37_79B9));
    while Instant::now() < until {
        let ids = run.ids.clone();
        let id = ids[r.range(0, ids.len() as u64 - 1) as usize];
        let Some(mut conn) = run.node(id).and_then(|n| SimConn::connect(&n)) else {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        };
        let key: ConnKey = run.next_key.fetch_add(1, Ordering::Relaxed);
        rec.open_conn(key, run.elapsed());
        let mut st = ClientState::new(seed, key);
        loop {
            if Instant::now() >= until {
                break;
            }
            let cmd = st.next(&mut r, &work, &mut lock(&known));
            let op = rec.begin(key, cmd.clone(), run.elapsed());
            match conn.call(&cmd, op_timeout(&cmd)).await {
                Some(resp) => {
                    let reply = to_reply(&cmd, resp);
                    rec.finish(op, run.elapsed(), reply.clone());
                    st.observe(&cmd, &reply, &mut lock(&known));
                }
                None => break,
            }
            if r.range(0, 29) == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(r.range(0, 150))).await;
        }
        rec.close_conn(key, run.elapsed());
        conn.close();
    }
}

/// Applies one fault.
async fn apply_fault(run: &Arc<RunShared>, f: &Fault) {
    let kind = match f {
        Fault::Net(a) => format!("{a:?}"),
        other => format!("{other:?}"),
    };
    let kind = kind
        .split(['(', ' '])
        .next()
        .unwrap_or_default()
        .to_string();
    *lock(&run.faults).entry(kind).or_default() += 1;
    match f {
        Fault::Net(a) => {
            run.event(format!("{a:?}"));
            run.net.apply(a);
        }
        Fault::Split(a, b) => {
            run.event(format!("split {a:?} | {b:?}"));
            for &x in a {
                for &y in b {
                    run.net.partition(x, y);
                }
            }
        }
        Fault::Crash(id) => run.crash(*id),
        Fault::CrashLeader => {
            if let Some(l) = run.leader() {
                run.crash(l.id);
            }
        }
        Fault::CrashAll => {
            for id in run.running() {
                run.crash(id);
            }
        }
        Fault::Restart(id) => {
            if let Err(e) = run.start(*id, false).await {
                run.problem(e);
            }
        }
        Fault::RestartAll => {
            for &id in &run.ids.clone() {
                if let Err(e) = run.start(id, false).await {
                    run.problem(e);
                }
            }
        }
        Fault::Wipe(id) => {
            run.crash(*id);
            if let Err(e) = run.start(*id, true).await {
                run.problem(e);
            }
        }
        Fault::Skew(id, ms) => {
            run.event(format!("skew node {id} {ms} ms"));
            if let Some(s) = run.skew.get(id) {
                s.store(ms * 1_000_000, Ordering::Relaxed);
            }
        }
    }
}

/// The result of one seed.
#[derive(Debug)]
pub struct Outcome {
    pub cfg: RunConfig,
    /// Everything that went wrong (empty: passed).
    pub failures: Vec<String>,
    pub report: Report,
    pub events: Vec<String>,
    pub virtual_time: Duration,
    pub history: History,
    /// Faults applied, by kind, plus `snapshot-installs`,
    /// `unacknowledged-ops`, `max-term`.
    pub stats: BTreeMap<String, u64>,
}

impl Outcome {
    pub fn passed(&self) -> bool {
        self.failures.is_empty()
    }

    pub fn describe(&self) -> String {
        let mut s = format!(
            "seed {} ({} nodes, {} clients, {:?} workload, max skew {} ms{}): {} \
             [replay: BSTK_CHAOS_SEED={} cargo test -p bstk-chaos --test inprocess replay -- \
             --ignored --nocapture]\n",
            self.cfg.seed,
            self.cfg.nodes,
            self.cfg.clients,
            self.cfg.duration,
            self.cfg.max_skew_ms,
            if self.cfg.wipe { ", wipes" } else { "" },
            if self.passed() { "ok" } else { "FAILED" },
            self.cfg.seed,
        );
        for f in &self.failures {
            s.push_str(&format!("  failure: {f}\n"));
        }
        if !self.passed() {
            s.push_str("  events:\n");
            for e in &self.events {
                s.push_str(&format!("    {e}\n"));
            }
        }
        s
    }
}

/// Runs one seed. Must run on a current-thread runtime with paused time
/// (`tokio::runtime::Builder::start_paused`, which needs tokio's
/// `test-util` feature: the test targets enable it, so that the library
/// itself, and the server binary built next to it, do not need it).
/// Leftover tasks (crashed incarnations, spawned forwards) must die with
/// the runtime (`shutdown_background`).
pub async fn run(cfg: RunConfig) -> Outcome {
    let seed = cfg.seed;
    let tmp = match tempfile::Builder::new().prefix("bstk-chaos-").tempdir() {
        Ok(t) => t,
        Err(e) => {
            return Outcome {
                cfg,
                failures: vec![format!("tempdir: {e}")],
                report: Report::default(),
                events: Vec::new(),
                virtual_time: Duration::ZERO,
                history: History::default(),
                stats: BTreeMap::new(),
            };
        }
    };
    let ids: Vec<NodeId> = (1..=cfg.nodes).collect();
    let raft_cfg = openraft::Config {
        cluster_name: "chaos".into(),
        heartbeat_interval: 50,
        election_timeout_min: 150,
        election_timeout_max: 300,
        install_snapshot_timeout: 2000,
        snapshot_policy: SnapshotPolicy::LogsSinceLast(40),
        max_in_snapshot_log_to_keep: 5,
        purge_batch_size: 1,
        replication_lag_threshold: 30,
        ..Default::default()
    };
    let raft_cfg = match raft_cfg.validate() {
        Ok(c) => Arc::new(c),
        Err(e) => {
            return Outcome {
                cfg,
                failures: vec![format!("raft config: {e}")],
                report: Report::default(),
                events: Vec::new(),
                virtual_time: Duration::ZERO,
                history: History::default(),
                stats: BTreeMap::new(),
            };
        }
    };
    let net = SimNetwork::new(seed, SimConfig::default());
    let run = Arc::new(RunShared {
        net: net.clone(),
        raft_cfg,
        t0: Instant::now(),
        ids: ids.clone(),
        skew: ids.iter().map(|&i| (i, AtomicI64::new(0))).collect(),
        ledger: Arc::default(),
        delivered: Arc::default(),
        dirs: ids
            .iter()
            .map(|&i| (i, tmp.path().join(format!("node{i}"))))
            .collect(),
        nodes: Mutex::new(BTreeMap::new()),
        problems: Mutex::new(Vec::new()),
        events: Mutex::new(Vec::new()),
        faults: Mutex::new(BTreeMap::new()),
        used_conns: Mutex::new(BTreeSet::new()),
        next_key: AtomicU64::new(1),
    });
    let rec = Recorder::new();
    let res = tokio::time::timeout(Duration::from_secs(600), drive(&run, &cfg, &rec)).await;
    if res.is_err() {
        run.problem("hang: the run did not finish within 600 virtual seconds".into());
    }
    let virtual_time = run.elapsed();
    for id in run.running() {
        run.crash(id);
    }
    let history = rec.history();
    let mut failures = lock(&run.problems).clone();
    failures.extend(lock(&run.ledger).problems.iter().cloned());
    let slack = Duration::from_millis(cfg.max_skew_ms + 5);
    let report = checker::check(
        &history,
        &CheckConfig {
            slack,
            ..CheckConfig::default()
        },
    );
    failures.extend(report.violations.iter().map(|v| v.to_string()));
    failures.extend(replay_check(&run, tmp.path()).await);
    let events = lock(&run.events).clone();
    let mut stats = lock(&run.faults).clone();
    stats.insert(
        "snapshot-installs".into(),
        lock(&run.ledger).snapshot_installs,
    );
    stats.insert(
        "unacknowledged-ops".into(),
        history.ops.iter().filter(|o| o.reply.is_none()).count() as u64,
    );
    stats.insert(
        "max-term".into(),
        lock(&run.ledger)
            .entries
            .values()
            .filter_map(|(b, _)| postcard::from_bytes::<Entry<TypeConfig>>(b).ok())
            .map(|e| e.log_id.leader_id.term)
            .max()
            .unwrap_or(0),
    );
    Outcome {
        cfg,
        failures,
        report,
        events,
        virtual_time,
        history,
        stats,
    }
}

/// Waits for `f` (polled every 20 ms) for at most `within`.
async fn wait_until(within: Duration, mut f: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + within;
    loop {
        if f() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn drive(run: &Arc<RunShared>, cfg: &RunConfig, rec: &Recorder) {
    let seed = cfg.seed;
    for &id in &run.ids {
        if let Err(e) = run.start(id, false).await {
            run.problem(e);
            return;
        }
    }
    let members: BTreeMap<NodeId, BasicNode> = run
        .ids
        .iter()
        .map(|&i| (i, BasicNode::new(format!("sim-{i}"))))
        .collect();
    if let Some(n1) = run.node(1)
        && let Err(e) = n1.raft.initialize(members).await
    {
        run.problem(format!("initialize: {e}"));
        return;
    }
    let all_up = |run: &RunShared| {
        let nodes = lock(&run.nodes);
        nodes.len() == run.ids.len() && nodes.values().all(|n| n.accepting.load(Ordering::Acquire))
    };
    if !wait_until(Duration::from_secs(20), || all_up(run)).await {
        run.problem("the cluster never started".into());
        return;
    }

    // Workload under faults.
    let start = Instant::now();
    let until = start + cfg.duration;
    let known: Arc<Mutex<Known>> = Arc::default();
    let mut clients = Vec::new();
    for i in 0..cfg.clients {
        clients.push(tokio::spawn(client(
            run.clone(),
            rec.clone(),
            known.clone(),
            cfg.work.clone(),
            seed,
            i as u64,
            until,
        )));
    }
    let gap = cfg.duration / (cfg.fault_steps as u32 + 1);
    let faults = generate_faults(
        seed,
        &run.ids,
        cfg.fault_steps,
        gap,
        cfg.max_skew_ms,
        cfg.wipe,
    );
    for (at, f) in &faults {
        tokio::time::sleep_until(start + *at).await;
        apply_fault(run, f).await;
    }

    // Heal and restart everything.
    tokio::time::sleep_until(until).await;
    run.event("heal".into());
    run.net.heal();
    run.net.set_drop(0.0);
    run.net.set_duplicate(0.0);
    run.net.set_delay(Duration::ZERO, Duration::ZERO);
    for &id in &run.ids.clone() {
        if let Err(e) = run.start(id, false).await {
            run.problem(e);
        }
    }
    if !wait_until(Duration::from_secs(30), || all_up(run)).await {
        run.problem(format!(
            "liveness: not every node accepts connections 30 s after healing: {}",
            cluster_view(run)
        ));
        return;
    }
    for c in clients {
        let _ = c.await;
    }
    // Every close is applied (queues empty), then every reservation and
    // delay has expired.
    if !wait_until(Duration::from_secs(30), || {
        lock(&run.nodes)
            .values()
            .all(|n| n.queue_len.load(Ordering::Relaxed) == 0)
    })
    .await
    {
        run.problem(format!(
            "liveness: owner queues not drained 30 s after healing: {}",
            cluster_view(run)
        ));
        return;
    }
    tokio::time::sleep(Duration::from_secs(u64::from(cfg.work.ttr.1) + 2)).await;
    verify(run, rec).await;
    // Converge: every node applied the same last index.
    let target = lock(&run.nodes)
        .values()
        .filter_map(|n| n.state.last_applied().map(|l| l.index))
        .max();
    if !wait_until(Duration::from_secs(20), || {
        lock(&run.nodes)
            .values()
            .all(|n| n.state.last_applied().map(|l| l.index) >= target)
    })
    .await
    {
        run.problem(format!(
            "liveness: replicas did not converge: {}",
            cluster_view(run)
        ));
    }
}

/// The final verification: `peek` and `stats-job` of every job ever seen,
/// then kick everything and drain the tube.
async fn verify(run: &Arc<RunShared>, rec: &Recorder) {
    let h = rec.history();
    let mut ids: BTreeSet<JobId> = BTreeSet::new();
    for o in &h.ops {
        if let Some(
            Reply::Inserted(id)
            | Reply::BuriedId(id)
            | Reply::Reserved { id, .. }
            | Reply::Found { id, .. },
        ) = o.acked()
        {
            ids.insert(*id);
        }
    }
    let Some(node) = run.leader().or_else(|| run.node(1)) else {
        run.problem("verification: no node".into());
        return;
    };
    let Some(mut conn) = SimConn::connect(&node) else {
        run.problem("verification: cannot connect".into());
        return;
    };
    let key: ConnKey = run.next_key.fetch_add(1, Ordering::Relaxed);
    rec.open_conn(key, run.elapsed());
    let mut cmds: Vec<Cmd> = Vec::new();
    for &id in &ids {
        cmds.push(Cmd::Peek(id));
        cmds.push(Cmd::StatsJob(id));
    }
    cmds.push(Cmd::Kick(1_000_000));
    let mut ok = true;
    for cmd in cmds {
        let op = rec.begin(key, cmd.clone(), run.elapsed());
        match conn.call(&cmd, Duration::from_secs(10)).await {
            Some(r) => rec.finish(op, run.elapsed(), to_reply(&cmd, r)),
            None => {
                ok = false;
                break;
            }
        }
    }
    // Drain.
    while ok {
        let cmd = Cmd::ReserveWithTimeout(0);
        let op = rec.begin(key, cmd.clone(), run.elapsed());
        let Some(r) = conn.call(&cmd, Duration::from_secs(10)).await else {
            ok = false;
            break;
        };
        let reply = to_reply(&cmd, r);
        rec.finish(op, run.elapsed(), reply.clone());
        let Reply::Reserved { id, .. } = reply else {
            break;
        };
        let cmd = Cmd::Delete(id);
        let op = rec.begin(key, cmd.clone(), run.elapsed());
        match conn.call(&cmd, Duration::from_secs(10)).await {
            Some(r) => rec.finish(op, run.elapsed(), to_reply(&cmd, r)),
            None => ok = false,
        }
    }
    rec.close_conn(key, run.elapsed());
    conn.close();
    if !ok {
        run.problem(format!(
            "liveness: a verification command got no reply: {}",
            cluster_view(run)
        ));
    }
}

fn cluster_view(run: &RunShared) -> String {
    let nodes = lock(&run.nodes);
    let mut s = String::new();
    if std::env::var("BSTK_CHAOS_DEBUG").is_ok() {
        for &a in &run.ids {
            for &b in &run.ids {
                let l = run.net.link_fault_log(a, b);
                if let Some(last) = l.last() {
                    s.push_str(&format!(
                        "[link {a}->{b}: {} faults, last {last:?}] ",
                        l.len()
                    ));
                }
            }
        }
        for (id, n) in nodes.iter() {
            let m = n.raft.metrics().borrow().clone();
            s.push_str(&format!("\n  node {id} metrics: {m:?}\n"));
        }
    }
    for (id, n) in nodes.iter() {
        let m = n.raft.metrics().borrow().clone();
        s.push_str(&format!(
            "[node {id}: {:?} {:?} term {} leader {:?} applied {:?} accepting {} queue {}] ",
            m.state,
            m.running_state,
            m.current_term,
            m.current_leader,
            m.last_applied.map(|l| l.index),
            n.accepting.load(Ordering::Relaxed),
            n.queue_len.load(Ordering::Relaxed),
        ));
    }
    s
}

/// Capture sink of the replay.
#[derive(Default)]
struct Capture {
    delivered: Mutex<BTreeMap<ConnId, Vec<Response>>>,
}

impl ReplySink for Capture {
    fn applied(&self, _: ConnId, _: u64) {}

    fn deliver(&self, conn: ConnId, resp: Response) {
        lock(&self.delivered).entry(conn).or_default().push(resp);
    }

    fn closed(&self, _: ConnId) {}
}

/// Replays the committed log on one fresh state machine per node and
/// compares what each connection received with what the replay sends it.
async fn replay_check(run: &RunShared, tmp: &Path) -> Vec<String> {
    let mut out = Vec::new();
    let entries = lock(&run.ledger).entries.clone();
    let mut decoded = Vec::new();
    for (expect, (i, (bytes, _))) in (0u64..).zip(&entries) {
        if *i != expect {
            out.push(format!("replay: no node applied log index {expect}"));
            return out;
        }
        match postcard::from_bytes::<Entry<TypeConfig>>(bytes) {
            Ok(e) => decoded.push(e),
            Err(e) => {
                out.push(format!("replay: undecodable entry {i}: {e}"));
                return out;
            }
        }
    }
    let delivered = lock(&run.delivered).clone();
    let owners: BTreeSet<NodeId> = delivered.keys().map(|&c| owner_of(c)).collect();
    for node in owners {
        let cap = Arc::new(Capture::default());
        let opts = SmOptions {
            node_id: node,
            engine: EngineConfig::default(),
            sys: Arc::new(|| Box::new(StaticSysInfo::default())),
            sink: cap.clone(),
        };
        let dir = tmp.join(format!("replay{node}"));
        let mut sm = match ClusterStateMachine::open(&dir, opts) {
            Ok(sm) => sm,
            Err(e) => {
                out.push(format!("replay: open: {e}"));
                return out;
            }
        };
        if let Err(e) = sm.apply(decoded.clone()).await {
            out.push(format!("replay: apply: {e}"));
            return out;
        }
        let want = lock(&cap.delivered).clone();
        for (conn, got) in delivered.iter().filter(|(c, _)| owner_of(**c) == node) {
            let exp = want.get(conn).map(Vec::as_slice).unwrap_or(&[]);
            if !exp.starts_with(got) {
                let n = got
                    .iter()
                    .zip(exp.iter())
                    .take_while(|(a, b)| a == b)
                    .count();
                out.push(format!(
                    "replay: connection {conn} received {:?} as reply #{n} but the replay \
                     sends {:?}",
                    got.get(n),
                    exp.get(n)
                ));
            }
        }
    }
    out
}
