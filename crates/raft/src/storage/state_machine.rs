//! The replicated state machine: an `Engine` plus the metadata needed to
//! apply [`Request`]s deterministically (openraft `RaftStateMachine`).
//!
//! # Apply rules
//!
//! - Every normal entry runs at `now = max(entry.now, last applied now)`,
//!   and that becomes the last applied now (also for ignored entries).
//! - The engine is created at the first normal entry, with that entry's
//!   `now` as its start time, so every node reports the same `uptime`
//!   (counted from the cluster's first entry). Before that, a placeholder
//!   engine answers the monitoring calls.
//! - `Op::Conn`: a `Connect` applies only with `seq == 1` and a local
//!   number above every local number of the same owner connected so far;
//!   any other input only if `seq` is the connection's next one. Anything
//!   else is ignored (`Applied { duplicate: true }`). `Disconnect` forgets
//!   the connection.
//! - `Op::Tick`, `Op::SetDraining` map to the engine inputs;
//!   `Op::DropNode(n)` disconnects every connection owned by `n` in
//!   ascending order.
//! - Blank and membership entries only update the metadata.
//!
//! # Reply routing
//!
//! After each engine call, the replies addressed to connections owned by
//! this node are handed to the [`ReplySink`], in order, after the batch's
//! state is published (the sink is called without any lock held). Other
//! nodes' replies are dropped. Each log entry is applied at most once per
//! process (openraft never re-applies at or below the last applied id; a
//! defensive check skips such entries), so a reply is delivered at most
//! once. After a restart, entries re-applied from the log address
//! connections of the previous process: the sink must ignore connections
//! it does not hold, and the server must number new connections above
//! [`StateHandle::highest_local`] (after catching up), which the dedup
//! rule requires anyway.
//!
//! Installing a snapshot at runtime skips the entries it covers, so their
//! replies to local connections are lost: the sink gets `closed` for every
//! local connection of the old and the new state.

use std::collections::BTreeMap;
use std::io::{self, Cursor};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use bstk_engine::{ConnId, Engine, EngineConfig, EngineInput, EngineState, Nanos, Outbox, SysInfo};
use bstk_proto::Response;
use openraft::storage::RaftStateMachine;
use openraft::{
    AnyError, BasicNode, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, OptionalSend,
    RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError, StorageIOError, StoredMembership,
};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use super::OpenError;
use super::snapshot::SnapshotStore;
use crate::{Applied, CONN_SEQ_BITS, NodeId, Op, Request, TypeConfig, owner_of};

type Sid = LogId<NodeId>;
type SResult<T> = Result<T, StorageError<NodeId>>;
type Membership = StoredMembership<NodeId, BasicNode>;

/// Receives what the local node must do for its own connections. Called
/// from the state machine task, in log order, without locks held; must
/// not block (hand off to a channel) and may be called for connections
/// the process does not hold (ignore those).
pub trait ReplySink: Send + Sync + 'static {
    /// Input `seq` of local connection `conn` was applied (it was not a
    /// duplicate). Called before the replies the entry produced.
    fn applied(&self, conn: ConnId, seq: u64);
    /// A reply for local connection `conn`.
    fn deliver(&self, conn: ConnId, resp: Response);
    /// Local connection `conn` is gone from the replicated state (a
    /// `Disconnect` or `DropNode` entry), or its replies were skipped by a
    /// snapshot install: close the socket.
    fn closed(&self, conn: ConnId);
}

/// Builds the `SysInfo` handed to each engine the state machine creates.
pub type SysFactory = Arc<dyn Fn() -> Box<dyn SysInfo> + Send + Sync>;

/// State machine construction parameters.
#[derive(Clone)]
pub struct SmOptions {
    /// This node (replies to its connections are delivered).
    pub node_id: NodeId,
    /// Engine configuration (`-z`, `-s`); must be the same on every node.
    /// `journal` is forced off.
    pub engine: EngineConfig,
    pub sys: SysFactory,
    pub sink: Arc<dyn ReplySink>,
}

/// Published after every applied batch (see [`StateHandle::subscribe`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AppliedInfo {
    pub last_applied: Option<Sid>,
    pub last_now: Nanos,
    pub next_deadline: Option<Nanos>,
}

/// Replicated metadata next to the engine, part of every snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub(crate) struct SmMeta {
    /// The engine was created (at the first normal entry).
    pub(crate) started: bool,
    pub(crate) last_now: Nanos,
    /// Next expected `seq` of every open connection.
    pub(crate) next_seq: BTreeMap<ConnId, u64>,
    /// Highest local connection number connected so far, per owner.
    pub(crate) highest_local: BTreeMap<NodeId, u64>,
}

/// Snapshot data as transferred between nodes.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SnapshotPayload {
    pub(crate) version: u32,
    pub(crate) meta: SmMeta,
    pub(crate) engine: EngineState,
}

const PAYLOAD_VERSION: u32 = 1;

fn local_of(conn: ConnId) -> u64 {
    conn & ((1 << CONN_SEQ_BITS) - 1)
}

struct Core {
    engine: Engine,
    meta: SmMeta,
    last_applied: Option<Sid>,
    membership: Membership,
}

impl Core {
    fn info(&self) -> AppliedInfo {
        AppliedInfo {
            last_applied: self.last_applied,
            last_now: self.meta.last_now,
            next_deadline: self.engine.next_deadline(),
        }
    }
}

struct Shared {
    node_id: NodeId,
    cfg: EngineConfig,
    sys: SysFactory,
    core: Mutex<Core>,
    info: watch::Sender<AppliedInfo>,
}

impl Shared {
    fn lock(&self) -> SResult<MutexGuard<'_, Core>> {
        self.core.lock().map_err(|_| {
            sm_err(
                ErrorVerb::Read,
                io::Error::other("state machine mutex poisoned"),
            )
        })
    }
}

/// Something the sink must hear about, collected during a batch.
enum Event {
    Applied(ConnId, u64),
    Deliver(ConnId, Response),
    Closed(ConnId),
}

fn dispatch(sink: &dyn ReplySink, events: Vec<Event>) {
    for ev in events {
        match ev {
            Event::Applied(c, s) => sink.applied(c, s),
            Event::Deliver(c, r) => sink.deliver(c, r),
            Event::Closed(c) => sink.closed(c),
        }
    }
}

fn sm_err(verb: ErrorVerb, e: io::Error) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::new(ErrorSubject::StateMachine, verb, AnyError::new(&e)),
    }
}

fn snap_err(
    meta: Option<&SnapshotMeta<NodeId, BasicNode>>,
    verb: ErrorVerb,
    e: io::Error,
) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::new(
            ErrorSubject::Snapshot(meta.map(|m| m.signature())),
            verb,
            AnyError::new(&e),
        ),
    }
}

fn invalid(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

/// Decode a snapshot payload and rebuild the engine, checking that the
/// metadata agrees with the engine state.
fn restore(payload: &[u8], sys: &SysFactory) -> io::Result<(Engine, SmMeta)> {
    let p: SnapshotPayload =
        postcard::from_bytes(payload).map_err(|e| invalid(format!("snapshot payload: {e}")))?;
    if p.version != PAYLOAD_VERSION {
        return Err(invalid(format!(
            "unsupported snapshot payload version {}",
            p.version
        )));
    }
    let engine = Engine::import_state(p.engine, sys()).map_err(|e| invalid(e.to_string()))?;
    let conns = engine.conn_ids();
    if !conns.iter().copied().eq(p.meta.next_seq.keys().copied()) {
        return Err(invalid(
            "snapshot connection metadata does not match the engine connections",
        ));
    }
    for (&c, &seq) in &p.meta.next_seq {
        let highest = p.meta.highest_local.get(&owner_of(c)).copied();
        if seq < 2 || highest.is_none_or(|h| local_of(c) > h) {
            return Err(invalid(format!(
                "snapshot connection {c} has inconsistent metadata"
            )));
        }
    }
    if !p.meta.started && !conns.is_empty() {
        return Err(invalid("snapshot has connections but no started engine"));
    }
    Ok((engine, p.meta))
}

fn encode_payload(engine: EngineState, meta: SmMeta) -> io::Result<Vec<u8>> {
    postcard::to_allocvec(&SnapshotPayload {
        version: PAYLOAD_VERSION,
        meta,
        engine,
    })
    .map_err(|e| invalid(format!("encode snapshot: {e}")))
}

/// openraft state machine (see the module docs). Also the snapshot builder.
#[derive(Clone)]
pub struct ClusterStateMachine {
    shared: Arc<Shared>,
    sink: Arc<dyn ReplySink>,
    snaps: Arc<Mutex<SnapshotStore>>,
}

/// Cheap, cloneable read access to the applied state for the server.
#[derive(Clone)]
pub struct StateHandle {
    shared: Arc<Shared>,
}

impl ClusterStateMachine {
    /// Lock `dir` (creating it if needed) and rebuild the state from the
    /// latest snapshot there, if any; openraft re-applies the log after it.
    pub fn open(dir: &Path, opts: SmOptions) -> Result<ClusterStateMachine, OpenError> {
        let snaps = SnapshotStore::open(dir)?;
        let mut cfg = opts.engine;
        cfg.journal = false;
        let core = match snaps.load_current()? {
            Some((meta, payload)) => {
                let (engine, sm_meta) = restore(&payload, &opts.sys).map_err(|e| {
                    OpenError::Corrupt(format!("snapshot {}: {e}", meta.snapshot_id))
                })?;
                Core {
                    engine,
                    meta: sm_meta,
                    last_applied: meta.last_log_id,
                    membership: meta.last_membership,
                }
            }
            None => Core {
                engine: Engine::new(0, cfg.clone(), (opts.sys)()),
                meta: SmMeta::default(),
                last_applied: None,
                membership: Membership::default(),
            },
        };
        let (info, _) = watch::channel(core.info());
        Ok(ClusterStateMachine {
            shared: Arc::new(Shared {
                node_id: opts.node_id,
                cfg,
                sys: opts.sys,
                core: Mutex::new(core),
                info,
            }),
            sink: opts.sink,
            snaps: Arc::new(Mutex::new(snaps)),
        })
    }

    /// A read handle for the server.
    pub fn handle(&self) -> StateHandle {
        StateHandle {
            shared: self.shared.clone(),
        }
    }

    fn snaps(&self) -> SResult<MutexGuard<'_, SnapshotStore>> {
        self.snaps.lock().map_err(|_| {
            snap_err(
                None,
                ErrorVerb::Read,
                io::Error::other("snapshot store mutex poisoned"),
            )
        })
    }
}

impl Core {
    fn route(&self, node: NodeId, out: &mut Outbox, events: &mut Vec<Event>) {
        for (c, r) in out.drain(..) {
            if owner_of(c) == node {
                events.push(Event::Deliver(c, r));
            }
        }
    }

    /// Apply one normal entry; returns whether it was ignored.
    fn apply_request(&mut self, sh: &Shared, req: Request, events: &mut Vec<Event>) -> bool {
        let now = req.now.max(self.meta.last_now);
        self.meta.last_now = now;
        if !self.meta.started {
            self.engine = Engine::new(now, sh.cfg.clone(), (sh.sys)());
            self.meta.started = true;
        }
        let node = sh.node_id;
        let mut out = Outbox::new();
        match req.op {
            Op::Conn { seq, input } => {
                let Some(conn) = input.conn() else {
                    tracing::warn!("ignoring a connection entry without a connection: {input:?}");
                    return true;
                };
                let owner = owner_of(conn);
                let ok = match input {
                    EngineInput::Connect(_) => {
                        seq == 1
                            && !self.meta.next_seq.contains_key(&conn)
                            && self
                                .meta
                                .highest_local
                                .get(&owner)
                                .is_none_or(|&h| local_of(conn) > h)
                    }
                    _ => self.meta.next_seq.get(&conn) == Some(&seq),
                };
                if !ok {
                    return true;
                }
                let disconnect = matches!(input, EngineInput::Disconnect(_));
                match input {
                    EngineInput::Connect(_) => {
                        self.meta.highest_local.insert(owner, local_of(conn));
                        self.meta.next_seq.insert(conn, 2);
                    }
                    EngineInput::Disconnect(_) => {
                        self.meta.next_seq.remove(&conn);
                    }
                    _ => {
                        self.meta.next_seq.insert(conn, seq + 1);
                    }
                }
                if owner == node {
                    events.push(Event::Applied(conn, seq));
                }
                self.engine.apply_input(now, input, &mut out);
                self.route(node, &mut out, events);
                if disconnect && owner == node {
                    events.push(Event::Closed(conn));
                }
            }
            Op::Tick => {
                self.engine.apply_input(now, EngineInput::Tick, &mut out);
                self.route(node, &mut out, events);
            }
            Op::SetDraining(on) => {
                self.engine
                    .apply_input(now, EngineInput::SetDraining(on), &mut out);
                self.route(node, &mut out, events);
            }
            Op::DropNode(n) => {
                let conns: Vec<ConnId> = self
                    .engine
                    .conn_ids()
                    .into_iter()
                    .filter(|&c| owner_of(c) == n)
                    .collect();
                for c in conns {
                    self.meta.next_seq.remove(&c);
                    self.engine
                        .apply_input(now, EngineInput::Disconnect(c), &mut out);
                    self.route(node, &mut out, events);
                    if n == node {
                        events.push(Event::Closed(c));
                    }
                }
            }
        }
        false
    }
}

impl StateHandle {
    fn core(&self) -> Option<MutexGuard<'_, Core>> {
        self.shared.core.lock().ok()
    }

    pub fn node_id(&self) -> NodeId {
        self.shared.node_id
    }

    pub fn last_applied(&self) -> Option<Sid> {
        self.shared.info.borrow().last_applied
    }

    /// Last applied `now` (engine time); the leader stamps new entries
    /// with at least this.
    pub fn last_now(&self) -> Nanos {
        self.shared.info.borrow().last_now
    }

    /// Earliest engine time at which a `Tick` entry is due, if any.
    pub fn next_deadline(&self) -> Option<Nanos> {
        self.shared.info.borrow().next_deadline
    }

    /// Watch the state published after every applied batch.
    pub fn subscribe(&self) -> watch::Receiver<AppliedInfo> {
        self.shared.info.subscribe()
    }

    /// Monitoring view of the engine (HTTP endpoints).
    pub fn snapshot_limited(&self, now: Nanos, max_tubes: usize) -> Option<bstk_engine::Snapshot> {
        self.core()
            .map(|c| c.engine.snapshot_limited(now, max_tubes))
    }

    /// Ids of all replicated connections, ascending.
    pub fn conn_ids(&self) -> Vec<ConnId> {
        self.core().map(|c| c.engine.conn_ids()).unwrap_or_default()
    }

    /// Last applied `seq` of `conn`, or `None` if the connection is not
    /// open in the replicated state (never connected, or disconnected).
    pub fn applied_seq(&self, conn: ConnId) -> Option<u64> {
        self.core()
            .and_then(|c| c.meta.next_seq.get(&conn).map(|s| s - 1))
    }

    /// Highest local connection number of `node` connected so far (0 if
    /// none). A node must number new connections above this.
    pub fn highest_local(&self, node: NodeId) -> u64 {
        self.core()
            .and_then(|c| c.meta.highest_local.get(&node).copied())
            .unwrap_or(0)
    }

    /// The last applied membership.
    pub fn membership(&self) -> Option<Membership> {
        self.core().map(|c| c.membership.clone())
    }

    /// Full engine state (diagnostics and tests).
    pub fn export_state(&self) -> Option<EngineState> {
        self.core().map(|c| c.engine.export_state())
    }

    #[cfg(test)]
    pub(crate) fn meta(&self) -> SmMeta {
        self.core().map(|c| c.meta.clone()).unwrap_or_default()
    }
}

impl RaftSnapshotBuilder<TypeConfig> for ClusterStateMachine {
    async fn build_snapshot(&mut self) -> SResult<Snapshot<TypeConfig>> {
        let (state, sm_meta, last_applied, membership) = {
            let c = self.shared.lock()?;
            (
                c.engine.export_state(),
                c.meta.clone(),
                c.last_applied,
                c.membership.clone(),
            )
        };
        let payload =
            encode_payload(state, sm_meta).map_err(|e| snap_err(None, ErrorVerb::Write, e))?;
        let mut snaps = self.snaps()?;
        let last = last_applied.map_or_else(|| "none".to_string(), |l| l.to_string());
        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership: membership,
            snapshot_id: snaps.new_id(&last),
        };
        snaps
            .save(&meta, &payload, false)
            .map_err(|e| snap_err(Some(&meta), ErrorVerb::Write, e))?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(payload)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for ClusterStateMachine {
    type SnapshotBuilder = ClusterStateMachine;

    async fn applied_state(&mut self) -> SResult<(Option<Sid>, Membership)> {
        let c = self.shared.lock()?;
        Ok((c.last_applied, c.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> SResult<Vec<Applied>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut events = Vec::new();
        let mut res = Vec::new();
        let info = {
            let mut c = self.shared.lock()?;
            for ent in entries {
                if c.last_applied.is_some_and(|l| ent.log_id.index <= l.index) {
                    tracing::warn!("skipping already applied log entry {}", ent.log_id);
                    res.push(Applied::default());
                    continue;
                }
                let duplicate = match ent.payload {
                    EntryPayload::Blank => false,
                    EntryPayload::Membership(m) => {
                        c.membership = StoredMembership::new(Some(ent.log_id), m);
                        false
                    }
                    EntryPayload::Normal(req) => c.apply_request(&self.shared, req, &mut events),
                };
                c.last_applied = Some(ent.log_id);
                res.push(Applied { duplicate });
            }
            c.info()
        };
        self.shared.info.send_replace(info);
        dispatch(self.sink.as_ref(), events);
        Ok(res)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> SResult<Box<Cursor<Vec<u8>>>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> SResult<()> {
        let payload = snapshot.into_inner();
        let (engine, sm_meta) = restore(&payload, &self.shared.sys)
            .map_err(|e| snap_err(Some(meta), ErrorVerb::Read, e))?;
        self.snaps()?
            .save(meta, &payload, true)
            .map_err(|e| snap_err(Some(meta), ErrorVerb::Write, e))?;
        let node = self.shared.node_id;
        let (events, info) = {
            let mut c = self.shared.lock()?;
            let mut local: Vec<ConnId> = c
                .engine
                .conn_ids()
                .into_iter()
                .chain(engine.conn_ids())
                .filter(|&x| owner_of(x) == node)
                .collect();
            local.sort_unstable();
            local.dedup();
            c.engine = engine;
            c.meta = sm_meta;
            c.last_applied = meta.last_log_id;
            c.membership = meta.last_membership.clone();
            (
                local.into_iter().map(Event::Closed).collect::<Vec<_>>(),
                c.info(),
            )
        };
        self.shared.info.send_replace(info);
        dispatch(self.sink.as_ref(), events);
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> SResult<Option<Snapshot<TypeConfig>>> {
        let snaps = self.snaps()?;
        match snaps.load_current() {
            Ok(None) => Ok(None),
            Ok(Some((meta, payload))) => Ok(Some(Snapshot {
                meta,
                snapshot: Box::new(Cursor::new(payload)),
            })),
            Err(e) => Err(snap_err(snaps.current_meta(), ErrorVerb::Read, e)),
        }
    }
}
