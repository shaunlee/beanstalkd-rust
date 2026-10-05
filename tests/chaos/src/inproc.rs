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
//! Per node incarnation this follows the server's cluster actor
//! (docs/DESIGN.md §8 "Forward queue"): one ordered queue of `(conn, seq,
//! input)` (`Connect` at seq 1, a put as `PutStarted` then `Command::Put`,
//! `Disconnect` on close); the leader proposes its unsent items, and every
//! forward it receives, as `Op::Batch` entries (`bstk_raft::split_batches`)
//! with `client_write_ff`; a follower forwards them to the leader in batches
//! over the simulated network (one in flight); an item leaves the queue when
//! the state machine reports it applied; everything unapplied is resent after
//! a leader or term change, a failed forward or a stall. The leader stamps
//! `now = max(clock, last applied now, last stamped)` and proposes `Tick` at
//! `next_deadline()`. A (re)started node waits until it has caught up, has
//! `DropNode(self, first_local - 1)` committed through the current leader,
//! and only then accepts connections, numbered from `first_local`: above an
//! emulated durable reservation (the `conn-ids` file, lost with a wipe),
//! everything the state has seen for the node, and the time floor.
//!
//! Membership-driven networking as in the server (docs/DESIGN.md §8, P6-T1):
//! each node admits peer requests (`SimNetwork::set_admits`) only from the
//! nodes of its effective and committed memberships, its forward handler
//! refuses non-members, a node outside its own effective membership accepts
//! no clients, and the leader proposes `DropNode` for every connection owner
//! that is not a member (again when its highest local number grows, and
//! every second while its connections remain).
//!
//! A node without Raft state (wiped, or a new node) and one restarted while
//! its emulated rejoin marker is still set runs the server's **discovery**
//! (docs/DESIGN.md §8 "Startup modes"): it asks every other node for its
//! `StatusEx` over the simulated network (`status::probe_ex`; each node
//! answers from a status source built like the server's: its effective
//! membership, applied index, highest member id and `rejoining` flag) until
//! the server's `status::startup_decision` decides. *Join* waits until the
//! operator adds it; *Refuse* ends the node (exit status 1 in the server);
//! *Rejoin* sets the marker, persists the adopted vote with `save_vote`,
//! re-checks the decision against the membership's voters, then starts Raft
//! with elections off and its vote gate closed until it has applied the
//! `DropNode(self)` a leader proposed for it after it started; then it clears
//! the marker, re-enables elections and opens the gate. While rejoining it
//! re-checks every 3 s whether its id was removed meanwhile.
//!
//! # Faults
//!
//! Partitions (pairs, one-way blocks, isolation of a node, minority /
//! majority splits), message loss, delay and duplication, paused nodes,
//! clock skew per node, crash (all `Raft` handles and the storage dropped
//! without `shutdown`) and restart from the same directory, crash of the
//! current leader, crash of every node, and a wiped node rejoining (its
//! directory emptied; it catches up through a snapshot install). A wipe is
//! skipped when it would leave fewer voters of the current membership with
//! their data than a quorum of each of its voter sets, or while it is joint.
//!
//! With [`RunConfig::membership`], the schedule also changes the membership
//! through the server's own executor (`bstk_raft::admin`, guardrails
//! included; see [`membership`]).
//!
//! # Checks
//!
//! - every applied log index has the same entry on every node and
//!   incarnation (committed entries are never lost or changed, also across
//!   crashes and restarts);
//! - every replica's engine state is identical at the same applied index
//!   (hash of `export_state` after each applied batch and each snapshot
//!   install), and so is the highest member id, which never decreases;
//! - the replies each connection received are a prefix of what a
//!   single-engine replay of the committed log sends it;
//! - the history checker on the replies the clients received;
//! - liveness: after healing, a leader serves, every member accepts
//!   connections and the final verification completes;
//! - the membership checks of [`membership`] (final membership uniform and
//!   explained by the requested changes, connection owners are members,
//!   removed nodes' connections closed in time, ids never reused, no vote
//!   adopted from a rejoining voter).

mod membership;

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use bstk_engine::{ConnId, EngineConfig, EngineInput, Nanos, StaticSysInfo};
use bstk_proto::{Command, Response};
use bstk_raft::forward::{ForwardError, ForwardHandler, ForwardTransport};
use bstk_raft::listener::VoteGate;
use bstk_raft::sim::{FaultAction, SimConfig, SimNetwork, SimRng};
use bstk_raft::status::{self, MembershipView, NodeStatusEx, StatusSource};
use bstk_raft::storage::{
    self, ClusterStateMachine, LogOptions, OpenError, ReplySink, SmOptions, StateHandle,
};
use bstk_raft::{
    Applied, CONN_SEQ_BITS, ForwardRequest, ForwardResponse, NodeId, Op, Request, TypeConfig,
    conn_id, owner_of,
};
use openraft::storage::{RaftLogStorage, RaftStateMachine};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, Raft, RaftMetrics, ServerState, Snapshot, SnapshotMeta,
    SnapshotPolicy, StorageError, StoredMembership,
};
use tokio::sync::{mpsc, watch};
use tokio::task::AbortHandle;
use tokio::time::Instant;

use crate::checker::{self, CheckConfig, Report};
use crate::history::{Cmd, ConnKey, History, JobId, Recorder, Reply};
use crate::workload::{ClientState, Known, WorkloadConfig};

pub use membership::MFault;

const ANCHOR: Nanos = 1_700_000_000_000_000_000;
/// A wiped incarnation (no emulated `conn-ids` reservation) starts its
/// connection numbers this far above the highest local number the state
/// has seen for the node: a stand-in for the server's time floor
/// (`unix_seconds << 16`), which needs wall-clock seconds.
/// The simulated wall clock's value when a run starts (seconds since the
/// Unix epoch).
const SIM_EPOCH_SECS: u64 = 1_800_000_000;
/// As the server's `durable::FLOOR_DELAY`.
const FLOOR_DELAY: Duration = Duration::from_secs(1);
/// Resend everything unapplied if the front item waits this long.
const STALL: Duration = Duration::from_secs(1);
/// As the server's leader duties (`duties::PERIOD`, `NON_MEMBER_RETRY`).
const MEMBER_CHECK: Duration = Duration::from_millis(100);
const NON_MEMBER_RETRY: Duration = Duration::from_secs(1);
/// As the server's `REJOIN_RECHECK`.
const REJOIN_RECHECK: Duration = Duration::from_secs(3);
/// As the server's discovery backoff (`PROBE_BACKOFF_MAX`).
/// The packaged unit's `RestartSec`: a node that exits on a fatal Raft stop
/// starts again after it.
/// What the openraft 0.9 race reports (`raft_core.rs`, `Command::Replicate`).
const KNOWN_FATAL: &str = "replication channel closed";
const SUPERVISOR_RESTART: Duration = Duration::from_secs(2);
const PROBE_BACKOFF_MAX: Duration = Duration::from_secs(2);

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

#[derive(Debug, Clone)]
pub struct RunConfig {
    pub seed: u64,
    /// Initial voters (ids `1..=nodes`).
    pub nodes: u64,
    pub clients: usize,
    pub duration: Duration,
    pub fault_steps: usize,
    pub max_skew_ms: u64,
    /// Include wiped-node rejoins in the schedule (on unless
    /// `BSTK_CHAOS_NO_WIPE` is set). A wiped node restarts in discovery
    /// (see the module docs).
    pub wipe: bool,
    /// Include membership changes in the schedule (see [`membership`]).
    pub membership: bool,
    /// Ids above the initial voters that membership changes may add.
    pub spares: u64,
    pub work: WorkloadConfig,
}

impl RunConfig {
    /// The fixed-membership mix.
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
            wipe: std::env::var("BSTK_CHAOS_NO_WIPE").is_err(),
            membership: false,
            spares: 0,
            work: WorkloadConfig::default(),
        }
    }

    /// The membership mix: the fixed mix's faults plus membership changes,
    /// over longer runs (a replace takes several steps).
    pub fn membership_from_seed(seed: u64) -> RunConfig {
        let mut c = RunConfig::from_seed(seed);
        let mut r = SimRng::new(seed ^ 0x3E3B_E25F);
        c.membership = true;
        c.spares = 6;
        c.duration = Duration::from_secs(r.range(12, 22));
        c.fault_steps = r.range(10, 18) as usize;
        c
    }

    pub fn ids(&self) -> Vec<NodeId> {
        (1..=self.nodes + self.spares).collect()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Fault {
    Net(FaultAction),
    Split(Vec<NodeId>, Vec<NodeId>),
    Crash(NodeId),
    CrashLeader,
    CrashAll,
    Restart(NodeId),
    RestartAll,
    Wipe(NodeId),
    Skew(NodeId, i64),
    Member(MFault),
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
    let mut out = Vec::new();
    for _ in 0..steps {
        at += Duration::from_millis(r.range(gap_ms / 2, gap_ms + gap_ms / 2));
        out.push((at, base_fault(&mut r, nodes, max_skew_ms, wipe)));
    }
    out
}

/// The membership mix: as [`generate_faults`] over every id (a fault on a
/// node that is not running does nothing), with about two faults in five
/// replaced by a membership change ([`membership::generate`]).
pub fn generate_membership_faults(
    seed: u64,
    nodes: &[NodeId],
    steps: usize,
    gap: Duration,
    max_skew_ms: u64,
    wipe: bool,
) -> Vec<(Duration, Fault)> {
    let mut r = SimRng::new(seed ^ 0xFA17_3E3B);
    let mut at = Duration::ZERO;
    let gap_ms = gap.as_millis() as u64;
    let mut out = Vec::new();
    for _ in 0..steps {
        at += Duration::from_millis(r.range(gap_ms / 2, gap_ms + gap_ms / 2));
        let f = if r.range(0, 4) < 2 {
            Fault::Member(membership::generate(&mut r))
        } else {
            base_fault(&mut r, nodes, max_skew_ms, wipe)
        };
        out.push((at, f));
    }
    out
}

fn base_fault(r: &mut SimRng, nodes: &[NodeId], max_skew_ms: u64, wipe: bool) -> Fault {
    let n = nodes.len() as u64;
    let pick = |r: &mut SimRng| nodes[r.range(0, n - 1) as usize];
    match r.range(0, 19) {
        0 => Fault::Net(FaultAction::Partition(pick(r), pick(r))),
        1 => Fault::Net(FaultAction::Block(pick(r), pick(r))),
        2 | 3 => Fault::Net(FaultAction::Isolate(pick(r), nodes.to_vec())),
        4 => {
            let mut v = nodes.to_vec();
            for i in (1..v.len()).rev() {
                let j = r.range(0, i as u64) as usize;
                v.swap(i, j);
            }
            let k = r.range(1, (n - 1) / 2) as usize;
            let (a, b) = v.split_at(k);
            Fault::Split(a.to_vec(), b.to_vec())
        }
        5 => Fault::Net(FaultAction::Pause(pick(r))),
        6 => Fault::Net(FaultAction::Resume(pick(r))),
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
        12 => Fault::Crash(pick(r)),
        13 => Fault::CrashLeader,
        14 => Fault::Restart(pick(r)),
        15 => Fault::RestartAll,
        16 if wipe => Fault::Wipe(pick(r)),
        16 => Fault::Restart(pick(r)),
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
            Fault::Skew(pick(r), s)
        }
    }
}

/// Everything every incarnation applied, by log index (see the module
/// docs' checks), plus what the membership checks need.
#[derive(Default)]
struct Ledger {
    entries: BTreeMap<u64, (Vec<u8>, NodeId)>,
    states: BTreeMap<u64, (u64, NodeId)>,
    /// The highest member id after applying up to an index.
    highest: BTreeMap<u64, (NodeId, NodeId)>,
    problems: Vec<String>,
    snapshot_installs: u64,
    /// Every node of every membership entry applied so far.
    ever: BTreeSet<NodeId>,
    /// Membership entry indexes seen applied.
    memberships: BTreeSet<u64>,
    /// The voter sets of the latest membership entry applied anywhere
    /// (committed).
    latest: Option<(u64, Vec<BTreeSet<NodeId>>)>,
    /// A removed node: the index of the entry that removed it and when it
    /// was first applied anywhere.
    removed: BTreeMap<NodeId, (u64, Duration)>,
    /// When a node that had applied a removal first held no connection of
    /// the removed node.
    released: BTreeMap<NodeId, Duration>,
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

    fn highest_member(&mut self, node: NodeId, index: u64, highest: NodeId) {
        match self.highest.get(&index) {
            None => {
                self.highest.insert(index, (highest, node));
            }
            Some(&(h, first)) if h != highest => self.problems.push(format!(
                "highest member id at applied index {index}: node {node} has {highest}, node \
                 {first} {h}"
            )),
            Some(_) => {}
        }
    }

    fn membership(
        &mut self,
        index: u64,
        nodes: BTreeSet<NodeId>,
        configs: Vec<BTreeSet<NodeId>>,
        now: Duration,
    ) {
        if !self.memberships.insert(index) {
            return;
        }
        if self.latest.as_ref().is_none_or(|(i, _)| *i < index) {
            self.latest = Some((index, configs));
        }
        for &x in self.ever.difference(&nodes) {
            self.removed.entry(x).or_insert((index, now));
        }
        self.ever.extend(nodes);
    }

    fn unreleased(&self, applied: u64) -> Vec<NodeId> {
        self.removed
            .iter()
            .filter(|(x, (i, _))| *i <= applied && !self.released.contains_key(x))
            .map(|(&x, _)| x)
            .collect()
    }
}

fn state_hash(h: &StateHandle) -> Option<u64> {
    let st = h.export_state()?;
    let bytes = postcard::to_allocvec(&st).ok()?;
    let mut s = DefaultHasher::new();
    bytes.hash(&mut s);
    Some(s.finish())
}

struct RecSm {
    inner: ClusterStateMachine,
    handle: StateHandle,
    node: NodeId,
    ledger: Arc<Mutex<Ledger>>,
    t0: Instant,
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
        type Members = (u64, BTreeSet<NodeId>, Vec<BTreeSet<NodeId>>);
        let members: Vec<Members> = v
            .iter()
            .filter_map(|e| match &e.payload {
                EntryPayload::Membership(m) => Some((
                    e.log_id.index,
                    m.nodes().map(|(&n, _)| n).collect(),
                    m.get_joint_config().clone(),
                )),
                _ => None,
            })
            .collect();
        let res = self.inner.apply(v).await?;
        let now = self.t0.elapsed();
        let mut l = lock(&self.ledger);
        let last = recs.last().map(|r| r.0);
        for (i, b) in recs {
            l.entry(self.node, i, b);
        }
        for (i, nodes, configs) in members {
            l.membership(i, nodes, configs, now);
        }
        if let Some(i) = last {
            if let Some(h) = state_hash(&self.handle) {
                l.state(self.node, i, h, "apply");
            }
            l.highest_member(self.node, i, self.handle.highest_member());
            let pending = l.unreleased(i);
            if !pending.is_empty() {
                let owners: BTreeSet<NodeId> =
                    self.handle.conn_ids().into_iter().map(owner_of).collect();
                for x in pending {
                    if !owners.contains(&x) {
                        l.released.insert(x, now);
                    }
                }
            }
        }
        Ok(res)
    }

    async fn get_snapshot_builder(&mut self) -> ClusterStateMachine {
        self.inner.get_snapshot_builder().await
    }

    async fn begin_receiving_snapshot(&mut self) -> SResult<Box<bstk_raft::SnapshotFile>> {
        self.inner.begin_receiving_snapshot().await
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<bstk_raft::SnapshotFile>,
    ) -> SResult<()> {
        self.inner.install_snapshot(meta, snapshot).await?;
        let mut l = lock(&self.ledger);
        l.snapshot_installs += 1;
        if let (Some(id), Some(h)) = (meta.last_log_id, state_hash(&self.handle)) {
            l.state(self.node, id.index, h, "snapshot install");
            l.highest_member(self.node, id.index, self.handle.highest_member());
        }
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> SResult<Option<Snapshot<TypeConfig>>> {
        self.inner.get_current_snapshot().await
    }
}

type ReplyTx = mpsc::UnboundedSender<Response>;

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

/// The nodes rejoining or in discovery (the `rejoining` flag of their
/// status answers), shared with the status sources.
#[derive(Default)]
struct Rejoining {
    /// The emulated durable rejoin marker: set when a node is wiped (it then
    /// has no data, like a node whose marker is not written yet) or decides
    /// to rejoin; survives crashes; cleared when the rejoin ends.
    marker: Mutex<BTreeSet<NodeId>>,
    /// Nodes whose discovery task runs.
    discovering: Mutex<BTreeSet<NodeId>>,
}

impl Rejoining {
    fn now(&self) -> BTreeSet<NodeId> {
        let mut s = lock(&self.marker).clone();
        s.extend(lock(&self.discovering).iter().copied());
        s
    }

    fn contains(&self, id: NodeId) -> bool {
        lock(&self.marker).contains(&id) || lock(&self.discovering).contains(&id)
    }
}

/// Answers status probes as the server's `StatusView` does: the durable
/// state from the log store; the membership, leader and applied index from
/// openraft's metrics once Raft runs, from storage before.
struct SimStatus {
    id: NodeId,
    log: storage::LogStore,
    state: StateHandle,
    rejoining: Arc<Rejoining>,
    startup_membership: StoredMembership<NodeId, BasicNode>,
    metrics: OnceLock<watch::Receiver<RaftMetrics<NodeId, BasicNode>>>,
}

impl StatusSource for SimStatus {
    fn status(&self) -> status::NodeStatus {
        self.log.status()
    }

    fn status_ex(&self) -> NodeStatusEx {
        let durable = self.log.status();
        let mut ex = NodeStatusEx::from_status(durable);
        ex.rejoining = self.rejoining.contains(self.id);
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
        let known = [durable.committed, ex.last_applied]
            .into_iter()
            .flatten()
            .map(|l| l.index)
            .max();
        let committed = membership
            .log_id()
            .is_some_and(|l| known.is_some_and(|k| k >= l.index));
        ex.membership = MembershipView::from_stored(&membership, committed);
        ex.highest_member = ex
            .membership
            .nodes
            .keys()
            .copied()
            .fold(ex.highest_member, NodeId::max);
        ex
    }
}

enum OwnerMsg {
    Input(ConnId, EngineInput),
}

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
    /// Every task of this incarnation, aborted when it crashes; `None`
    /// once crashed (a task registered later is aborted at once).
    tasks: Mutex<Option<Vec<AbortHandle>>>,
    run: Arc<RunShared>,
    rejoin: Option<Arc<VoteGate>>,
    /// The membership executor's lock (`bstk_raft::admin`).
    admin_lock: Arc<tokio::sync::Mutex<()>>,
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

    /// The effective membership (the latest in this node's log).
    fn effective(&self) -> Arc<StoredMembership<NodeId, BasicNode>> {
        self.raft
            .server_metrics()
            .borrow()
            .membership_config
            .clone()
    }

    /// Whether this node admits `node`'s peer requests (as the server's
    /// `Core::is_member`).
    fn admits(&self, node: NodeId) -> bool {
        match lock(&self.run.admitted).get(&self.id) {
            Some((a, _)) => a.contains(&node),
            None => self.effective().nodes().any(|(&n, _)| n == node),
        }
    }

    fn track(&self, h: AbortHandle) {
        match lock(&self.tasks).as_mut() {
            Some(t) => t.push(h),
            None => h.abort(),
        }
    }
}

struct Fwd(Arc<NodeInc>);

impl ForwardHandler for Fwd {
    async fn forward(&self, req: ForwardRequest) -> ForwardResponse {
        let inc = &self.0;
        // As the server's handler: a request racing the allowlist update
        // must not reach the proposer.
        if !inc.admits(req.from) {
            return ForwardResponse::NotLeader { leader: None };
        }
        if !inc.is_leader() {
            let leader = inc.raft.metrics().borrow().current_leader;
            return ForwardResponse::NotLeader { leader };
        }
        let items = req
            .items
            .into_iter()
            .map(|(_, seq, input)| (seq, input))
            .collect();
        for batch in bstk_raft::split_batches(items) {
            if !inc.propose(Op::Batch(batch)).await {
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

/// The leader's non-member duty (as the server's
/// `duties::drop_non_members`): `DropNode` for every connection owner
/// outside the effective membership, again when its highest local number
/// grows past the bound last proposed or [`NON_MEMBER_RETRY`] later.
async fn drop_non_members(inc: &NodeInc, outsiders: &mut HashMap<NodeId, (u64, Instant)>) -> bool {
    let m = inc.effective();
    let owners = inc.state.connection_owners();
    outsiders.retain(|n, _| owners.contains_key(n));
    for (node, highest) in owners {
        if node == inc.id || m.nodes().any(|(&n, _)| n == node) {
            continue;
        }
        let due = outsiders
            .get(&node)
            .is_none_or(|&(bound, at)| highest > bound || at.elapsed() >= NON_MEMBER_RETRY);
        if !due {
            continue;
        }
        if !inc
            .propose(Op::DropNode {
                node,
                up_to_local: highest,
            })
            .await
        {
            return false;
        }
        outsiders.insert(node, (highest, Instant::now()));
    }
    true
}

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
    let mut outsiders: HashMap<NodeId, (u64, Instant)> = HashMap::new();
    let mut members_checked: Option<(Option<LogId<NodeId>>, Instant)> = None;
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
                if inc.is_leader() {
                    let mlog = *inc.effective().log_id();
                    if members_checked.is_none_or(|(l, at)| l != mlog || at.elapsed() >= MEMBER_CHECK) {
                        members_checked = Some((mlog, Instant::now()));
                        if !drop_non_members(&inc, &mut outsiders).await {
                            return;
                        }
                    }
                } else {
                    outsiders.clear();
                    members_checked = None;
                }
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
            let items: Vec<(u64, EngineInput)> = queue
                .iter()
                .skip(cursor)
                .map(|i| (i.seq, i.input.clone()))
                .collect();
            cursor = queue.len();
            for batch in bstk_raft::split_batches(items) {
                if !inc.propose(Op::Batch(batch)).await {
                    return;
                }
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
        let h = tokio::spawn(async move {
            let res: Result<ForwardResponse, ForwardError> = net.forward(target, req).await;
            let _ = tx.send((e, res));
        });
        inc.track(h.abort_handle());
    }
}

/// The server's `exit_on_fatal` and the service manager: when Raft stops on
/// the openraft 0.9 race of docs/DESIGN.md §8 ("Fatal Raft stop"), the
/// process exits and starts again after [`SUPERVISOR_RESTART`]. Any other
/// fatal error is a finding.
fn supervise(inc: Arc<NodeInc>) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
    // Boxed: the restart calls `start`, which spawns this function again,
    // and the opaque future types would be recursive.
    Box::pin(async move {
        let id = inc.id;
        let Some(e) = bstk_raft::fatal::wait_fatal(inc.raft.metrics()).await else {
            return;
        };
        let run = inc.run.clone();
        if !e.to_string().contains(KNOWN_FATAL) {
            run.problem(format!("node {id}: Raft stopped on a fatal error: {e}"));
            return;
        }
        let current = lock(&run.nodes)
            .get(&id)
            .is_some_and(|n| Arc::ptr_eq(n, &inc));
        drop(inc);
        if !current {
            return;
        }
        run.event(format!(
            "node {id} exits: Raft stopped on a fatal error ({e})"
        ));
        run.count("RaftFatalExit");
        lock(&run.disruptions).push(run.elapsed());
        // The restart outlives this incarnation's tasks, which `crash` aborts.
        tokio::spawn(async move {
            run.crash(id);
            tokio::time::sleep(SUPERVISOR_RESTART).await;
            if run.ended.load(Ordering::Relaxed) || !run.restartable().contains(&id) {
                return;
            }
            if let Err(e) = run.start(id, StartMode::Normal).await {
                run.problem(e);
            }
        });
    })
}

/// Follows a node's memberships into its allowlist (as the server's
/// `membership::watch`): the nodes of its effective and committed
/// memberships once it has one, and for the listener (not the forward
/// handler) every id above the highest member it knows of.
async fn watch_members(inc: Arc<NodeInc>) {
    loop {
        let eff = inc.effective();
        let Ok(committed) = inc
            .raft
            .with_raft_state(|st| {
                st.membership_state
                    .committed()
                    .nodes()
                    .map(|(&n, _)| n)
                    .collect::<BTreeSet<NodeId>>()
            })
            .await
        else {
            return;
        };
        let mut allow: BTreeSet<NodeId> = eff.nodes().map(|(&n, _)| n).collect();
        if !allow.is_empty() {
            allow.extend(committed);
            let above = allow
                .iter()
                .copied()
                .fold(inc.state.highest_member(), NodeId::max);
            lock(&inc.run.admitted).insert(inc.id, (allow, above));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

type Allowlists = BTreeMap<NodeId, (BTreeSet<NodeId>, NodeId)>;

/// How a node is started.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StartMode {
    /// An initial voter at bootstrap (node 1 initializes the membership).
    Bootstrap,
    /// A (re)start: Raft on its state, or discovery without state or with
    /// the rejoin marker.
    Normal,
    /// Wipe the data directory first (then discovery).
    Wipe,
}

pub(crate) struct RunShared {
    net: SimNetwork,
    raft_cfg: Arc<openraft::Config>,
    t0: Instant,
    membership: bool,
    /// The initial voters.
    initial: Vec<NodeId>,
    /// The initial voters and the spares.
    all_ids: Vec<NodeId>,
    skew: BTreeMap<NodeId, AtomicI64>,
    ledger: Arc<Mutex<Ledger>>,
    delivered: Arc<Mutex<BTreeMap<ConnId, Vec<Response>>>>,
    dirs: BTreeMap<NodeId, PathBuf>,
    nodes: Mutex<BTreeMap<NodeId, Arc<NodeInc>>>,
    /// Nodes in discovery before they start Raft: start generation and
    /// task.
    starting: Mutex<BTreeMap<NodeId, (u64, AbortHandle)>>,
    next_start: AtomicU64,
    problems: Mutex<Vec<String>>,
    events: Mutex<Vec<String>>,
    faults: Mutex<BTreeMap<String, u64>>,
    used_conns: Mutex<BTreeSet<ConnId>>,
    rejoining: Arc<Rejoining>,
    /// Emulated durable connection-number reservation per node (the
    /// server's `conn-ids` file): above every local number handed out.
    /// Lost with a wipe.
    conn_ids: Mutex<BTreeMap<NodeId, u64>>,
    next_key: AtomicU64,
    /// Each running node's allowlist (absent: no membership yet, admits
    /// every node) and the id above which its listener admits any node,
    /// shared with the network's admit checks.
    admitted: Arc<Mutex<Allowlists>>,
    /// Nodes started at least once.
    started: Mutex<BTreeSet<NodeId>>,
    /// Removed nodes the operator stopped: never restarted, except by
    /// [`MFault::RestartRemoved`].
    retired: Mutex<BTreeSet<NodeId>>,
    /// Nodes whose discovery refused their id (the server exits).
    refused: Mutex<BTreeSet<NodeId>>,
    /// The next spare id to add.
    next_spare: AtomicU64,
    /// Every membership change requested, with its outcome.
    attempts: Mutex<Vec<membership::Attempt>>,
    /// When faults were applied (and the final heal): a removed node's
    /// connections must be gone within the bound after the later of its
    /// removal and the last of these.
    disruptions: Mutex<Vec<Duration>>,
    /// Set when the run is over, so that no supervised restart starts a node.
    ended: AtomicBool,
    /// Jobs each client connection holds reserved: `(node, count)`.
    holding: Mutex<HashMap<ConnKey, (NodeId, usize)>>,
    /// Operator tasks (membership changes in progress).
    operators: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    /// A partition or one-way block is in place (until the next heal).
    partitioned: AtomicBool,
}

impl RunShared {
    fn elapsed(&self) -> Duration {
        self.t0.elapsed()
    }

    fn clock(&self, id: NodeId) -> Nanos {
        let base = ANCHOR.saturating_add(self.elapsed().as_nanos() as u64);
        let skew = self.skew.get(&id).map_or(0, |s| s.load(Ordering::Relaxed));
        base.saturating_add_signed(skew)
    }

    fn node(&self, id: NodeId) -> Option<Arc<NodeInc>> {
        lock(&self.nodes).get(&id).cloned()
    }

    fn running(&self) -> Vec<NodeId> {
        let mut ids: BTreeSet<NodeId> = lock(&self.nodes).keys().copied().collect();
        ids.extend(lock(&self.starting).keys().copied());
        ids.into_iter().collect()
    }

    /// The node that leads in the highest term, preferring one a quorum
    /// acknowledged within the last second (as the server's
    /// `leader_reachable`): a node cut off, or removed and restarted with
    /// its data, may still believe it leads an older term, with an old view
    /// of what is committed.
    fn leader(&self) -> Option<Arc<NodeInc>> {
        let nodes = lock(&self.nodes);
        let acked = |n: &NodeInc| {
            let m = n.raft.metrics().borrow().clone();
            m.millis_since_quorum_ack.is_some_and(|ms| ms <= 1000)
                || m.membership_config.membership().voter_ids().count() == 1
        };
        let term = |n: &NodeInc| n.raft.metrics().borrow().current_term;
        let leaders: Vec<&Arc<NodeInc>> = nodes.values().filter(|n| n.is_leader()).collect();
        leaders
            .iter()
            .filter(|n| acked(n))
            .max_by_key(|n| term(n))
            .or_else(|| leaders.iter().max_by_key(|n| term(n)))
            .map(|n| Arc::clone(n))
    }

    /// The nodes a (re)start of every node starts: the initial voters in
    /// the fixed mix; every node started before, not retired and not
    /// refused, in the membership mix.
    fn restartable(&self) -> Vec<NodeId> {
        if !self.membership {
            return self.initial.clone();
        }
        let retired = lock(&self.retired).clone();
        let refused = lock(&self.refused).clone();
        lock(&self.started)
            .iter()
            .copied()
            .filter(|n| !retired.contains(n) && !refused.contains(n))
            .collect()
    }

    fn problem(&self, p: String) {
        lock(&self.problems).push(p);
    }

    fn count(&self, kind: &str) {
        *lock(&self.faults).entry(kind.to_string()).or_default() += 1;
    }

    /// `unix_seconds << 16` on the simulated clock (virtual time since the
    /// run started, on top of a fixed epoch), as the server's time floor.
    fn time_floor(&self) -> u64 {
        (SIM_EPOCH_SECS + self.t0.elapsed().as_secs()) << 16
    }

    fn event(&self, e: String) {
        let t = self.elapsed();
        lock(&self.events).push(format!("{t:?} {e}"));
    }

    fn crash(&self, id: NodeId) {
        if let Some((_, task)) = lock(&self.starting).remove(&id) {
            task.abort();
            lock(&self.rejoining.discovering).remove(&id);
            self.net.unregister(id);
            self.event(format!("crash node {id} (still probing)"));
        }
        let Some(inc) = lock(&self.nodes).remove(&id) else {
            return;
        };
        self.event(format!("crash node {id}"));
        inc.accepting.store(false, Ordering::Release);
        self.net.unregister(id);
        lock(&self.admitted).remove(&id);
        if let Some(tasks) = lock(&inc.tasks).take() {
            for t in tasks {
                t.abort();
            }
        }
        lock(&inc.clients).clear();
        // Stops the Raft core (which owns the storage) even while a request
        // still in flight on the simulated network holds a handle to it: a
        // dead process answers nothing, and its storage lock must be free
        // for the next incarnation.
        let raft = inc.raft.clone();
        drop(inc);
        tokio::spawn(async move {
            let _ = raft.shutdown().await;
        });
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

    /// Starts node `id` (see [`StartMode`]). A node in discovery probes its
    /// peers in a background task first (see the module docs).
    async fn start(self: &Arc<Self>, id: NodeId, mode: StartMode) -> Result<(), String> {
        if lock(&self.nodes).contains_key(&id) || lock(&self.starting).contains_key(&id) {
            return Ok(());
        }
        let clients: Arc<Mutex<HashMap<ConnId, ReplyTx>>> = Arc::default();
        let (applied_tx, applied_rx) = mpsc::unbounded_channel();
        let sink = Arc::new(Sink {
            clients: clients.clone(),
            applied: applied_tx,
            delivered: self.delivered.clone(),
        });
        let dir = self.dirs.get(&id).ok_or("no data dir")?.clone();
        if mode == StartMode::Wipe {
            let s = self.open_storage(id, sink.clone()).await?;
            drop(s);
            wipe_dir(&dir).map_err(|e| format!("wipe node {id}: {e}"))?;
            lock(&self.conn_ids).remove(&id);
            lock(&self.rejoining.marker).insert(id);
            self.event(format!("wiped node {id}"));
        }
        lock(&self.started).insert(id);
        let has_state = storage::has_state(&dir).map_err(|e| format!("node {id}: {e}"))?;
        let discover = mode != StartMode::Bootstrap
            && (!has_state || lock(&self.rejoining.marker).contains(&id));
        let (mut log, mut sm) = self.open_storage(id, sink).await?;
        // What a status probe reports before Raft runs, as the server: the
        // latest membership in storage, as Raft will load it.
        let startup_membership = {
            let m = openraft::storage::StorageHelper::new(&mut log, &mut sm)
                .get_membership()
                .await
                .map_err(|e| format!("node {id}: read membership: {e}"))?;
            let e = m.effective();
            StoredMembership::new(*e.log_id(), e.membership().clone())
        };
        let status = Arc::new(SimStatus {
            id,
            log: log.clone(),
            state: sm.handle(),
            rejoining: self.rejoining.clone(),
            startup_membership,
            metrics: OnceLock::new(),
        });
        self.net.set_status_source(id, Some(status.clone()));
        let parts = Parts {
            log,
            sm,
            clients,
            applied_rx,
            status,
        };
        if !discover {
            return self.launch(id, parts, None).await;
        }
        let generation = self.next_start.fetch_add(1, Ordering::Relaxed);
        lock(&self.rejoining.discovering).insert(id);
        let mut starting = lock(&self.starting);
        let run = self.clone();
        let task = tokio::spawn(async move {
            if let Err(e) = run.discover(id, generation, parts).await {
                run.problem(e);
            }
        });
        starting.insert(id, (generation, task.abort_handle()));
        drop(starting);
        self.event(format!("node {id} probes its peers (discovery)"));
        Ok(())
    }

    /// Discovery (see the module docs), as the server's `discover`.
    async fn discover(
        self: &Arc<Self>,
        id: NodeId,
        generation: u64,
        mut parts: Parts,
    ) -> Result<(), String> {
        let mut local = parts.log.status().vote;
        let targets: BTreeSet<NodeId> = self.all_ids.iter().copied().filter(|&p| p != id).collect();
        let mut answers: BTreeMap<NodeId, NodeStatusEx> = BTreeMap::new();
        // Whether each answer's node was rejoining before and after its
        // probe round, by the harness's own record (the adoption check).
        let mut truth: BTreeMap<NodeId, (bool, bool)> = BTreeMap::new();
        let mut delay = Duration::from_millis(50);
        loop {
            let (got, truth_now) = self.probe(id, &targets).await;
            answers.extend(got);
            truth.extend(truth_now);
            match status::startup_decision(id, &answers, local) {
                status::Startup::Rejoin { vote, membership } => {
                    if lock(&self.retired).contains(&id) {
                        self.problem(format!(
                            "node {id} was removed but discovery decided to rejoin with \
                             membership {:?}",
                            membership.log_id
                        ));
                    }
                    lock(&self.rejoining.marker).insert(id);
                    self.check_adoption(id, &answers, &truth, &membership, local, vote);
                    if let Some(v) = &vote {
                        parts
                            .log
                            .save_vote(v)
                            .await
                            .map_err(|e| format!("node {id}: save vote: {e}"))?;
                    }
                    let voters = membership.voters();
                    let (again, truth_again) = self.probe(id, &voters).await;
                    match status::startup_decision(id, &again, vote) {
                        status::Startup::Rejoin {
                            vote: v2,
                            membership: m2,
                        } if m2.log_id == membership.log_id => {
                            self.check_adoption(id, &again, &truth_again, &m2, vote, v2);
                            if let Some(v) = v2.filter(|v2| Some(*v2) != vote) {
                                parts
                                    .log
                                    .save_vote(&v)
                                    .await
                                    .map_err(|e| format!("node {id}: save vote: {e}"))?;
                            }
                            let vote = v2.or(vote);
                            self.event(format!(
                                "node {id} adopted vote {vote:?} (membership {:?})",
                                membership.log_id
                            ));
                            lock(&self.rejoining.discovering).remove(&id);
                            return self.launch(id, parts, Some(generation)).await;
                        }
                        _ => {
                            local = vote.or(local);
                            answers = again;
                            truth = truth_again;
                        }
                    }
                }
                status::Startup::Refuse(reason) => {
                    let mut starting = lock(&self.starting);
                    if starting.get(&id).map(|e| e.0) == Some(generation) {
                        starting.remove(&id);
                        lock(&self.rejoining.discovering).remove(&id);
                        self.net.unregister(id);
                        lock(&self.refused).insert(id);
                        drop(starting);
                        self.event(format!("node {id} refused to start: {reason}"));
                        self.count("Refused");
                    }
                    return Ok(());
                }
                status::Startup::Join { .. } => {
                    if lock(&self.retired).contains(&id) {
                        self.problem(format!("node {id} was removed but discovery decided join"));
                    }
                }
                status::Startup::Wait { fresh, .. } => {
                    if fresh {
                        answers.clear();
                        truth.clear();
                    }
                }
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(PROBE_BACKOFF_MAX);
        }
    }

    /// One round of `StatusEx` probes, with whether each node that answered
    /// was rejoining (before or after the round, so a race can only make an
    /// answer count as rejoining).
    async fn probe(
        &self,
        id: NodeId,
        targets: &BTreeSet<NodeId>,
    ) -> (
        BTreeMap<NodeId, NodeStatusEx>,
        BTreeMap<NodeId, (bool, bool)>,
    ) {
        let before = self.rejoining.now();
        let (got, _) = status::probe_ex(&self.net.node(id), id, targets).await;
        let after = self.rejoining.now();
        let truth = got
            .keys()
            .map(|&p| (p, (before.contains(&p), after.contains(&p))))
            .collect();
        (got, truth)
    }

    /// Invariant 6: a rejoining node adopts no vote above every vote of the
    /// answers it may adopt from (voters of the membership, holding it, not
    /// rejoining) and its own; and no answer hides a rejoin (a node
    /// rejoining before and after its probe round answers `rejoining`).
    fn check_adoption(
        &self,
        id: NodeId,
        answers: &BTreeMap<NodeId, NodeStatusEx>,
        truth: &BTreeMap<NodeId, (bool, bool)>,
        m: &MembershipView,
        local: Option<openraft::Vote<NodeId>>,
        adopted: Option<openraft::Vote<NodeId>>,
    ) {
        let voters = m.voters();
        let rejoining = |n: &NodeId| truth.get(n).is_some_and(|&(b, a)| b && a);
        for (n, a) in answers {
            if rejoining(n) && !a.rejoining {
                self.problem(format!(
                    "invariant 6: node {n} answered node {id}'s probe as not rejoining while it was"
                ));
            }
        }
        let ceiling = answers
            .iter()
            .filter(|(n, a)| {
                voters.contains(n)
                    && **n != id
                    && !a.rejoining
                    && !rejoining(n)
                    && a.membership.log_id == m.log_id
            })
            .filter_map(|(_, a)| a.status.vote)
            .chain(local)
            .reduce(|a, b| if b > a { b } else { a });
        if let Some(v) = adopted
            && ceiling.is_none_or(|c| v > c)
        {
            self.problem(format!(
                "invariant 6: node {id} adopted vote {v} above every vote of the voters it may \
                 adopt from ({ceiling:?}): a rejoining voter's vote was adopted"
            ));
        }
    }

    /// Starts Raft on opened storage and registers the node. `rejoin`: the
    /// generation of the discovery start (from `starting`), `None`
    /// otherwise.
    async fn launch(
        self: &Arc<Self>,
        id: NodeId,
        parts: Parts,
        rejoin_start: Option<u64>,
    ) -> Result<(), String> {
        let Parts {
            log,
            sm,
            clients,
            applied_rx,
            status,
        } = parts;
        let rejoin = rejoin_start.is_some();
        let handle = sm.handle();
        let rec = RecSm {
            inner: sm,
            handle: handle.clone(),
            node: id,
            ledger: self.ledger.clone(),
            t0: self.t0,
        };
        // Rejoin mode, as the server: no elections from the start, inbound
        // votes refused.
        let cfg = if rejoin {
            let mut c = (*self.raft_cfg).clone();
            c.enable_elect = false;
            Arc::new(c)
        } else {
            self.raft_cfg.clone()
        };
        let raft = Raft::new(id, cfg, self.net.node(id), log, rec)
            .await
            .map_err(|e| format!("node {id}: raft: {e}"))?;
        // A discovery start registers while holding `starting`, so a crash
        // either aborted it before this point or finds the node running.
        let mut starting = lock(&self.starting);
        if let Some(generation) = rejoin_start {
            if starting.get(&id).map(|e| e.0) != Some(generation) {
                return Ok(());
            }
            starting.remove(&id);
        }
        let _ = status.metrics.set(raft.metrics());
        let gate = rejoin.then(|| Arc::new(VoteGate::new(false)));
        self.net.set_vote_gate(id, gate.clone());
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
            tasks: Mutex::new(Some(Vec::new())),
            run: self.clone(),
            rejoin: gate,
            admin_lock: Arc::new(tokio::sync::Mutex::new(())),
        });
        self.net
            .register(id, raft, Some(Arc::new(Fwd(inc.clone()))));
        let tasks = [
            tokio::spawn(owner(inc.clone(), owner_rx, applied_rx)),
            tokio::spawn(startup(inc.clone())),
            tokio::spawn(watch_members(inc.clone())),
            tokio::spawn(supervise(inc.clone())),
            tokio::spawn(bstk_raft::admin::finish_joint(inc.clone())),
        ];
        for t in tasks {
            inc.track(t.abort_handle());
        }
        lock(&self.nodes).insert(id, inc);
        drop(starting);
        self.event(format!(
            "start node {id}{}",
            if rejoin { " in rejoin mode" } else { "" }
        ));
        Ok(())
    }
}

struct Parts {
    log: storage::LogStore,
    sm: ClusterStateMachine,
    clients: Arc<Mutex<HashMap<ConnId, ReplyTx>>>,
    applied_rx: mpsc::UnboundedReceiver<(ConnId, u64)>,
    status: Arc<SimStatus>,
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
/// connections, then accept clients. A rejoining node also checks every
/// [`REJOIN_RECHECK`] whether its id was removed meanwhile, and ends (as the
/// server's process exits) if so.
async fn startup(inc: Arc<NodeInc>) {
    let id = inc.id;
    let started = Instant::now();
    let mut checked = Instant::now();
    let mut first: Option<u64> = None;
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
        // As the server's control handler: a leader proposes a node's
        // `DropNode` only for a member.
        if caught_up && first.is_none() {
            // As the server (`durable::first_local`): above the durable
            // reservation, everything the state has seen for this node, and
            // the time floor, which a node without a reservation (a wiped
            // one) takes at least `FLOOR_DELAY` after it started. Taken
            // before the `DropNode` it bounds.
            let highest = inc.state.highest_local(id);
            let persisted = lock(&inc.run.conn_ids).get(&id).copied();
            if persisted.is_none() {
                tokio::time::sleep_until(started + FLOOR_DELAY).await;
            }
            first = Some(
                persisted
                    .unwrap_or(0)
                    .max(highest + 1)
                    .max(inc.run.time_floor()),
            );
        }
        if caught_up
            && let Some(first) = first
            && let Some(leader) = inc.run.leader()
            && leader.admits(id)
        {
            let op = Op::DropNode {
                node: id,
                up_to_local: first - 1,
            };
            let req = Request {
                now: leader.stamp(),
                op,
            };
            let res =
                tokio::time::timeout(Duration::from_secs(1), leader.raft.client_write(req)).await;
            drop(leader);
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
                    if let Some(gate) = &inc.rejoin {
                        // An index learned from a leader after this
                        // incarnation started is applied: leave rejoin mode.
                        lock(&inc.run.rejoining.marker).remove(&id);
                        inc.raft.runtime_config().elect(true);
                        gate.open();
                        inc.run
                            .event(format!("node {id} left rejoin mode at index {want}"));
                    }
                    inc.next_local.store(first, Ordering::Release);
                    inc.accepting.store(true, Ordering::Release);
                    return;
                }
            }
        }
        if inc.rejoin.is_some() && checked.elapsed() >= REJOIN_RECHECK {
            checked = Instant::now();
            let targets: BTreeSet<NodeId> = inc.run.all_ids.iter().copied().collect();
            let (answers, _) = status::probe_ex(&inc.run.net.node(id), id, &targets).await;
            if let status::Startup::Refuse(reason) = status::startup_decision(id, &answers, None) {
                inc.run.event(format!(
                    "node {id} cannot finish rejoining and ends: {reason}"
                ));
                inc.run.count("RefusedWhileRejoining");
                lock(&inc.run.refused).insert(id);
                let run = inc.run.clone();
                drop(inc);
                run.crash(id);
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

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
        // As the server's readiness: a node outside its own effective
        // membership (removed, not told) is sent no clients.
        if !inc.effective().nodes().any(|(&n, _)| n == inc.id) {
            return None;
        }
        let local = inc.next_local.fetch_add(1, Ordering::AcqRel);
        let conn = conn_id(inc.id, local);
        {
            let mut ids = lock(&inc.run.conn_ids);
            let e = ids.entry(inc.id).or_insert(0);
            *e = (*e).max(local + 1);
        }
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

fn op_timeout(cmd: &Cmd) -> Duration {
    match cmd {
        Cmd::ReserveWithTimeout(t) => Duration::from_secs(u64::from(*t) + 5),
        _ => Duration::from_secs(5),
    }
}

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
        let ids = if run.membership {
            run.running()
        } else {
            run.initial.clone()
        };
        if ids.is_empty() {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }
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
                    lock(&run.holding).insert(key, (id, st.holds()));
                }
                None => break,
            }
            if r.range(0, 29) == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(r.range(0, 150))).await;
        }
        lock(&run.holding).remove(&key);
        rec.close_conn(key, run.elapsed());
        conn.close();
    }
}

async fn apply_fault(run: &Arc<RunShared>, f: &Fault) {
    let kind = match f {
        Fault::Net(a) => format!("{a:?}"),
        Fault::Member(m) => format!("{m:?}"),
        other => format!("{other:?}"),
    };
    let kind = kind
        .split(['(', ' ', '{'])
        .next()
        .unwrap_or_default()
        .to_string();
    *lock(&run.faults).entry(kind).or_default() += 1;
    if !matches!(f, Fault::Skew(..)) {
        lock(&run.disruptions).push(run.elapsed());
    }
    match f {
        Fault::Net(a) => {
            run.event(format!("{a:?}"));
            match a {
                FaultAction::Partition(..) | FaultAction::Block(..) | FaultAction::Isolate(..) => {
                    run.partitioned.store(true, Ordering::Relaxed);
                }
                FaultAction::Heal => run.partitioned.store(false, Ordering::Relaxed),
                _ => {}
            }
            run.net.apply(a);
        }
        Fault::Split(a, b) => {
            run.event(format!("split {a:?} | {b:?}"));
            run.partitioned.store(true, Ordering::Relaxed);
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
            if run.restartable().contains(id)
                && let Err(e) = run.start(*id, StartMode::Normal).await
            {
                run.problem(e);
            }
        }
        Fault::RestartAll => {
            for id in run.restartable() {
                if let Err(e) = run.start(id, StartMode::Normal).await {
                    run.problem(e);
                }
            }
        }
        Fault::Wipe(id) => {
            if !run.restartable().contains(id) {
                return;
            }
            if let Err(why) = membership::wipe_allowed(run, *id) {
                run.event(format!("wipe of node {id} skipped ({why})"));
                run.count("WipeSkipped");
                return;
            }
            run.crash(*id);
            if let Err(e) = run.start(*id, StartMode::Wipe).await {
                run.problem(e);
            }
        }
        Fault::Skew(id, ms) => {
            run.event(format!("skew node {id} {ms} ms"));
            if let Some(s) = run.skew.get(id) {
                s.store(ms * 1_000_000, Ordering::Relaxed);
            }
        }
        // In the background, as an operator would: a change waits for a
        // leader and may take seconds, and the schedule goes on meanwhile.
        Fault::Member(m) => {
            let (run2, m) = (run.clone(), m.clone());
            let h = tokio::spawn(async move { membership::apply(&run2, &m).await });
            lock(&run.operators).push(h);
        }
    }
}

#[derive(Debug)]
pub struct Outcome {
    pub cfg: RunConfig,
    pub failures: Vec<String>,
    pub report: Report,
    pub events: Vec<String>,
    pub virtual_time: Duration,
    pub history: History,
    /// Faults applied, by kind, plus `snapshot-installs`,
    /// `unacknowledged-ops`, `max-term` and the membership counts.
    pub stats: BTreeMap<String, u64>,
}

impl Outcome {
    pub fn passed(&self) -> bool {
        self.failures.is_empty()
    }

    pub fn describe(&self) -> String {
        let var = if self.cfg.membership {
            "churn_seed"
        } else {
            "replay"
        };
        let mut s = format!(
            "seed {} ({} nodes{}, {} clients, {:?} workload, max skew {} ms{}): {} \
             [replay: BSTK_CHAOS_SEED={} cargo test -p bstk-chaos --test inprocess {var} -- \
             --ignored --nocapture]\n",
            self.cfg.seed,
            self.cfg.nodes,
            if self.cfg.membership {
                format!(" + {} spares, membership changes", self.cfg.spares)
            } else {
                String::new()
            },
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
    let initial: Vec<NodeId> = (1..=cfg.nodes).collect();
    let all_ids = cfg.ids();
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
    let admitted: Arc<Mutex<Allowlists>> = Arc::default();
    for &id in &all_ids {
        let a = admitted.clone();
        net.set_admits(
            id,
            Some(Arc::new(move |from| {
                lock(&a)
                    .get(&id)
                    .is_none_or(|(s, above)| s.contains(&from) || from > *above)
            })),
        );
    }
    let run = Arc::new(RunShared {
        net: net.clone(),
        raft_cfg,
        t0: Instant::now(),
        membership: cfg.membership,
        initial: initial.clone(),
        all_ids: all_ids.clone(),
        skew: all_ids.iter().map(|&i| (i, AtomicI64::new(0))).collect(),
        ledger: Arc::default(),
        delivered: Arc::default(),
        dirs: all_ids
            .iter()
            .map(|&i| (i, tmp.path().join(format!("node{i}"))))
            .collect(),
        nodes: Mutex::new(BTreeMap::new()),
        starting: Mutex::new(BTreeMap::new()),
        next_start: AtomicU64::new(1),
        problems: Mutex::new(Vec::new()),
        events: Mutex::new(Vec::new()),
        faults: Mutex::new(BTreeMap::new()),
        used_conns: Mutex::new(BTreeSet::new()),
        rejoining: Arc::default(),
        conn_ids: Mutex::new(BTreeMap::new()),
        next_key: AtomicU64::new(1),
        admitted,
        started: Mutex::new(BTreeSet::new()),
        retired: Mutex::new(BTreeSet::new()),
        refused: Mutex::new(BTreeSet::new()),
        next_spare: AtomicU64::new(cfg.nodes + 1),
        attempts: Mutex::new(Vec::new()),
        disruptions: Mutex::new(Vec::new()),
        ended: AtomicBool::new(false),
        holding: Mutex::new(HashMap::new()),
        operators: Mutex::new(Vec::new()),
        partitioned: AtomicBool::new(false),
    });
    let rec = Recorder::new();
    let res = tokio::time::timeout(Duration::from_secs(900), drive(&run, &cfg, &rec)).await;
    if res.is_err() {
        run.problem("hang: the run did not finish within 900 virtual seconds".into());
    }
    let virtual_time = run.elapsed();
    for h in lock(&run.operators).drain(..) {
        h.abort();
    }
    run.ended.store(true, Ordering::Relaxed);
    for id in run.running() {
        run.crash(id);
    }
    // Break the network's references to the nodes' admit checks.
    for &id in &all_ids {
        net.set_admits(id, None);
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
    failures.extend(membership::check_log(&run, &initial));
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
    membership::stats(&run, &mut stats);
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

/// Every node of `members` runs, and only those, and every one accepts
/// connections.
fn all_up(run: &RunShared, members: &BTreeSet<NodeId>) -> bool {
    let nodes = lock(&run.nodes);
    nodes.keys().copied().collect::<BTreeSet<_>>() == *members
        && nodes.values().all(|n| n.accepting.load(Ordering::Acquire))
}

async fn drive(run: &Arc<RunShared>, cfg: &RunConfig, rec: &Recorder) {
    let seed = cfg.seed;
    for &id in &run.initial {
        if let Err(e) = run.start(id, StartMode::Bootstrap).await {
            run.problem(e);
            return;
        }
    }
    let members: BTreeMap<NodeId, BasicNode> = run
        .initial
        .iter()
        .map(|&i| (i, BasicNode::new(membership::addr_of(i, 0))))
        .collect();
    if let Some(n1) = run.node(1)
        && let Err(e) = n1.raft.initialize(members).await
    {
        run.problem(format!("initialize: {e}"));
        return;
    }
    let initial: BTreeSet<NodeId> = run.initial.iter().copied().collect();
    if !wait_until(Duration::from_secs(20), || all_up(run, &initial)).await {
        run.problem("the cluster never started".into());
        return;
    }

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
    let faults = if cfg.membership {
        generate_membership_faults(
            seed,
            &run.all_ids,
            cfg.fault_steps,
            gap,
            cfg.max_skew_ms,
            cfg.wipe,
        )
    } else {
        generate_faults(
            seed,
            &run.initial,
            cfg.fault_steps,
            gap,
            cfg.max_skew_ms,
            cfg.wipe,
        )
    };
    for (at, f) in &faults {
        tokio::time::sleep_until(start + *at).await;
        apply_fault(run, f).await;
    }

    tokio::time::sleep_until(until).await;
    run.event("heal".into());
    lock(&run.disruptions).push(run.elapsed());
    run.net.heal();
    run.partitioned.store(false, Ordering::Relaxed);
    run.net.set_drop(0.0);
    run.net.set_duplicate(0.0);
    run.net.set_delay(Duration::ZERO, Duration::ZERO);
    let members = if run.membership {
        match membership::settle(run).await {
            Some(m) => m,
            None => return,
        }
    } else {
        for &id in &run.initial {
            if let Err(e) = run.start(id, StartMode::Normal).await {
                run.problem(e);
            }
        }
        initial
    };
    if !wait_until(Duration::from_secs(30), || all_up(run, &members)).await {
        run.problem(format!(
            "liveness: not every member accepts connections 30 s after healing (members \
             {members:?}): {}",
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
    membership::check_final(run, &members).await;
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
    let first = lock(&run.nodes).values().next().cloned();
    let Some(node) = run.leader().or(first) else {
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
        for &a in &run.all_ids {
            for &b in &run.all_ids {
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
            "[node {id}: {:?} {:?} term {} leader {:?} applied {:?} accepting {} queue {} \
             membership {:?} {:?}] ",
            m.state,
            m.running_state,
            m.current_term,
            m.current_leader,
            m.last_applied.map(|l| l.index),
            n.accepting.load(Ordering::Relaxed),
            n.queue_len.load(Ordering::Relaxed),
            m.membership_config.log_id().map(|l| l.index),
            m.membership_config.membership().get_joint_config(),
        ));
    }
    let starting: Vec<NodeId> = lock(&run.starting).keys().copied().collect();
    if !starting.is_empty() {
        s.push_str(&format!("[in discovery: {starting:?}] "));
    }
    s
}

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
