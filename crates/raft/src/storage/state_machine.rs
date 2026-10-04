//! The replicated state machine: an `Engine` plus the metadata needed to
//! apply [`Request`]s deterministically (openraft `RaftStateMachine`).
//!
//! # Apply rules
//!
//! - Every normal entry runs at `now = max(entry.now, last applied now)`,
//!   which becomes the last applied now (also for ignored entries).
//! - The engine is created at the first normal entry with that entry's `now`
//!   as its start time, so every node reports the same `uptime`. Before that,
//!   a placeholder engine answers the monitoring calls.
//! - `Op::Conn` and the items of `Op::Batch` follow the dedup rules of
//!   [`crate::Op::Conn`] (a `Connect` needs `seq == 1` and a new local number,
//!   anything else the connection's next `seq`); an ignored input is
//!   `Applied { duplicate: true }`, and a batch counts as ignored only if every
//!   item was. `Disconnect` forgets the connection.
//! - `Op::Tick` and `Op::SetDraining` map to engine inputs; `Op::DropNode {
//!   node, up_to_local }` disconnects `node`'s connections with a local number
//!   `<= up_to_local`, in ascending order.
//! - Blank and membership entries only update the metadata.
//!
//! # Reply routing
//!
//! After each engine call, replies for connections owned by this node go to
//! the [`ReplySink`], in order, after the batch's state is published (no lock
//! held); other nodes' replies are not built (`Engine::set_local_conns`). An
//! entry is applied at most once per process (openraft never re-applies at or
//! below the last applied id, and a defensive check skips such entries), so a
//! reply is delivered at most once. After a restart, entries re-applied from
//! the log address connections of the previous process: the sink must ignore
//! connections it does not hold, and the server must number new connections
//! above [`StateHandle::highest_local`].
//!
//! Installing a snapshot at runtime skips the entries it covers, so their
//! replies to local connections are lost: the sink gets `closed` for every
//! local connection of the old and the new state.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

use bstk_engine::{
    ConnId, Engine, EngineConfig, EngineInput, EngineState, EngineStateView, LocalConns, Nanos,
    Outbox, SysInfo,
};
use bstk_proto::Response;
use openraft::storage::RaftStateMachine;
use openraft::{
    AnyError, BasicNode, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, OptionalSend,
    RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError, StorageIOError, StoredMembership,
};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use super::OpenError;
use super::snapshot::{HEADER_LEN, SnapshotStore};
use crate::snapshot_file::DEFAULT_MAX_SNAPSHOT_BYTES;
use crate::{Applied, CONN_SEQ_BITS, NodeId, Op, Request, SnapshotFile, TypeConfig, owner_of};

#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub mod fuzzing;

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
    fn deliver(&self, conn: ConnId, resp: Response);
    /// Local connection `conn` is gone from the replicated state (a
    /// `Disconnect` or `DropNode` entry), or its replies were skipped by a
    /// snapshot install: close the socket.
    fn closed(&self, conn: ConnId);
}

pub type SysFactory = Arc<dyn Fn() -> Box<dyn SysInfo> + Send + Sync>;

#[derive(Clone)]
pub struct SmOptions {
    pub node_id: NodeId,
    /// Engine configuration (`-z`, `-s`); must be the same on every node.
    /// `journal` is forced off.
    pub engine: EngineConfig,
    pub sys: SysFactory,
    pub sink: Arc<dyn ReplySink>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AppliedInfo {
    pub last_applied: Option<Sid>,
    pub last_now: Nanos,
    pub next_deadline: Option<Nanos>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub(crate) struct SmMeta {
    pub(crate) started: bool,
    pub(crate) last_now: Nanos,
    pub(crate) next_seq: BTreeMap<ConnId, u64>,
    pub(crate) highest_local: BTreeMap<NodeId, u64>,
}

/// Snapshot data as transferred between nodes. Written from
/// [`SnapshotPayloadRef`] and read field by field (`restore_from`); the
/// owned form is what tests build and inspect.
#[cfg(test)]
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SnapshotPayload {
    pub(crate) version: u32,
    pub(crate) meta: SmMeta,
    pub(crate) engine: EngineState,
}

/// [`SnapshotPayload`] borrowed from the live state: encodes to the same
/// bytes (postcard structs are their fields in order, and
/// `EngineStateView` encodes as `EngineState`).
#[derive(Serialize)]
struct SnapshotPayloadRef<'a> {
    version: u32,
    meta: &'a SmMeta,
    engine: EngineStateView<'a>,
}

/// Bumped whenever the encoding of `EngineState` or `SmMeta` changes
/// (postcard is positional, so old and new layouts do not decode as each
/// other). 2: P4-T3 buried / reservation maps. (P4-T5c streams the same
/// encoding to and from files; the version did not change.)
pub(crate) const PAYLOAD_VERSION: u32 = 2;

/// Every engine of this state machine builds replies only for its own
/// node's connections (docs/DESIGN.md §8a).
fn local_conns(node: NodeId) -> LocalConns {
    LocalConns::new(CONN_SEQ_BITS, node)
}

fn new_engine(now: Nanos, cfg: EngineConfig, sys: Box<dyn SysInfo>, node: NodeId) -> Engine {
    let mut engine = Engine::new(now, cfg, sys);
    engine.set_local_conns(Some(local_conns(node)));
    engine
}

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

/// Bytes beyond `-z` a single decoded item (job body, tube name) may take:
/// the decoder's scratch buffer is bounded by `-z` plus this.
const DECODE_MARGIN: usize = 64 << 10;

/// A postcard input flavor over a buffered reader (P4-T5c: payloads are
/// decoded from the file, not from a copy in memory). Byte strings are
/// copied through a scratch buffer of at most `limit` bytes, so a hostile
/// length cannot allocate more; borrowing from the input is not supported
/// (nothing in the payload borrows: `Bytes` and `String` take owned
/// copies). The first I/O error is kept, as postcard only reports
/// "unexpected end".
struct ReadFlavor<R> {
    r: R,
    scratch: Vec<u8>,
    limit: usize,
    consumed: u64,
    err: Option<io::Error>,
}

impl<R: BufRead> ReadFlavor<R> {
    fn fill(&mut self, n: usize) -> postcard::Result<()> {
        if n > self.limit {
            self.err.get_or_insert_with(|| {
                invalid(format!(
                    "snapshot item of {n} bytes exceeds the limit of {}",
                    self.limit
                ))
            });
            return Err(postcard::Error::DeserializeUnexpectedEnd);
        }
        if self.scratch.len() < n {
            self.scratch.resize(n, 0);
        }
        match self.r.read_exact(&mut self.scratch[..n]) {
            Ok(()) => {
                self.consumed += n as u64;
                Ok(())
            }
            Err(e) => {
                self.err.get_or_insert(e);
                Err(postcard::Error::DeserializeUnexpectedEnd)
            }
        }
    }
}

impl<'de, R: BufRead + 'de> postcard::de_flavors::Flavor<'de> for ReadFlavor<R> {
    type Remainder = Self;
    type Source = ();

    fn pop(&mut self) -> postcard::Result<u8> {
        self.fill(1)?;
        Ok(self.scratch[0])
    }

    fn try_take_n(&mut self, _ct: usize) -> postcard::Result<&'de [u8]> {
        self.err
            .get_or_insert_with(|| invalid("snapshot payload: borrowed data is not supported"));
        Err(postcard::Error::DeserializeUnexpectedEnd)
    }

    fn try_take_n_temp<'a>(&'a mut self, ct: usize) -> postcard::Result<&'a [u8]>
    where
        'de: 'a,
    {
        self.fill(ct)?;
        Ok(&self.scratch[..ct])
    }

    fn finalize(self) -> postcard::Result<Self> {
        Ok(self)
    }
}

/// Lets the snapshot payload be checksummed in the same pass that decodes
/// it, instead of a second read of a file that can be GiB.
struct CrcReader<R> {
    r: R,
    crc: u32,
}

impl<R: Read> Read for CrcReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.r.read(buf)?;
        self.crc = crc32c::crc32c_append(self.crc, &buf[..n]);
        Ok(n)
    }
}

/// Counts and checksums what is written through it, keeping the first
/// error (postcard reports any as "buffer full").
struct CrcWriter<W> {
    w: W,
    crc: u32,
    len: u64,
    err: Option<io::Error>,
}

impl<W: Write> Write for CrcWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.w.write(buf) {
            Ok(n) => {
                self.crc = crc32c::crc32c_append(self.crc, &buf[..n]);
                self.len += n as u64;
                Ok(n)
            }
            Err(e) => {
                let copy = io::Error::new(e.kind(), e.to_string());
                self.err.get_or_insert(e);
                Err(copy)
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.w.flush()
    }
}

fn write_payload(f: &File, meta: &SmMeta, engine: &Engine) -> io::Result<(u64, u32)> {
    let w = CrcWriter {
        w: f,
        crc: 0,
        len: 0,
        err: None,
    };
    let payload = SnapshotPayloadRef {
        version: PAYLOAD_VERSION,
        meta,
        engine: engine.state_view(),
    };
    let mut bw = BufWriter::with_capacity(1 << 16, w);
    let res = postcard::to_io(&payload, &mut bw).map(|_| ());
    let flushed = bw.flush();
    let w = bw.into_inner().map_err(|e| e.into_error())?;
    if let Some(e) = w.err {
        return Err(e);
    }
    res.map_err(|e| invalid(format!("encode snapshot: {e}")))?;
    flushed?;
    Ok((w.len, w.crc))
}

/// Decode the payload of `len` bytes that `r` is positioned at and
/// rebuild the engine (see [`check_restored`]); returns it with the
/// payload's CRC-32C, continued from `crc_seed`. The payload must be
/// exactly `len` bytes. Peak memory is the restored state plus one
/// pointer per job and the scratch buffer (`-z` plus a margin).
fn restore_from(
    r: impl Read,
    len: u64,
    crc_seed: u32,
    cfg: &EngineConfig,
    sys: &SysFactory,
    node: NodeId,
) -> io::Result<(Engine, SmMeta, u32)> {
    let reader = BufReader::with_capacity(
        1 << 16,
        CrcReader {
            r: r.take(len),
            crc: crc_seed,
        },
    );
    let flavor = ReadFlavor {
        r: reader,
        scratch: Vec::new(),
        limit: cfg.max_job_size as usize + DECODE_MARGIN,
        consumed: 0,
        err: None,
    };
    let mut de = postcard::Deserializer::from_flavor(flavor);
    let decoded = (|| {
        // The version is the first field: check it before decoding the
        // rest, whose layout depends on it.
        let version = u32::deserialize(&mut de)?;
        if version != PAYLOAD_VERSION {
            return Ok(Err(invalid(format!(
                "unsupported snapshot payload version {version}"
            ))));
        }
        let meta = SmMeta::deserialize(&mut de)?;
        let state = EngineState::deserialize(&mut de)?;
        Ok(Ok((meta, state)))
    })();
    let mut flavor = de
        .finalize()
        .map_err(|e| invalid(format!("snapshot payload: {e}")))?;
    let (meta, state) = match decoded {
        Ok(r) => r?,
        Err(e) => {
            let e: postcard::Error = e;
            return Err(flavor
                .err
                .take()
                .unwrap_or_else(|| invalid(format!("snapshot payload: {e}"))));
        }
    };
    if flavor.consumed != len {
        return Err(invalid(format!(
            "snapshot payload has {} bytes after its end",
            len - flavor.consumed
        )));
    }
    let crc = flavor.r.into_inner().crc;
    let (engine, meta) = check_restored(state, meta, cfg, sys, node)?;
    Ok((engine, meta, crc))
}

/// Rebuild the engine from a decoded payload, checking that the metadata
/// agrees with the engine state and that the snapshot's engine
/// configuration agrees with the local one (`cfg`): the snapshot must not
/// change this node's `-z` (the engine answers `JOB_TOO_BIG` by it, so a
/// difference would make nodes diverge), and it must not turn the journal
/// on (cluster mode has no binlog).
fn check_restored(
    state: EngineState,
    meta: SmMeta,
    cfg: &EngineConfig,
    sys: &SysFactory,
    node: NodeId,
) -> io::Result<(Engine, SmMeta)> {
    let mut engine = Engine::import_state(state, sys()).map_err(|e| invalid(e.to_string()))?;
    engine.set_local_conns(Some(local_conns(node)));
    let theirs = engine.config();
    if theirs.max_job_size != cfg.max_job_size {
        return Err(invalid(format!(
            "snapshot max_job_size {} differs from this node's {} \
             (every node must use the same -z)",
            theirs.max_job_size, cfg.max_job_size
        )));
    }
    if theirs.journal {
        return Err(invalid("snapshot engine has the journal enabled"));
    }
    let conns = engine.conn_ids();
    if !conns.iter().copied().eq(meta.next_seq.keys().copied()) {
        return Err(invalid(
            "snapshot connection metadata does not match the engine connections",
        ));
    }
    for (&c, &seq) in &meta.next_seq {
        let highest = meta.highest_local.get(&owner_of(c)).copied();
        if seq < 2 || highest.is_none_or(|h| local_of(c) > h) {
            return Err(invalid(format!(
                "snapshot connection {c} has inconsistent metadata"
            )));
        }
    }
    if !meta.started && !conns.is_empty() {
        return Err(invalid("snapshot has connections but no started engine"));
    }
    Ok((engine, meta))
}

/// The checksum comes out of the decode pass for free; it also guards a
/// file that changed after `SnapshotStore::open` verified it.
fn restore_current(
    snaps: &SnapshotStore,
    cfg: &EngineConfig,
    sys: &SysFactory,
    node: NodeId,
) -> io::Result<Option<(super::snapshot::Meta, Engine, SmMeta)>> {
    let Some((meta, mut f, layout)) = snaps.open_current()? else {
        return Ok(None);
    };
    f.seek(SeekFrom::Start(layout.payload_off))?;
    let (engine, sm_meta, crc) =
        restore_from(&mut f, layout.payload_len, layout.crc_seed, cfg, sys, node)?;
    if !layout.crc_matches(crc) {
        return Err(invalid("snapshot checksum mismatch"));
    }
    Ok(Some((meta, engine, sm_meta)))
}

#[derive(Clone)]
pub struct ClusterStateMachine {
    shared: Arc<Shared>,
    sink: Arc<dyn ReplySink>,
    snaps: Arc<Mutex<SnapshotStore>>,
    max_snapshot_bytes: u64,
}

#[derive(Clone)]
pub struct StateHandle {
    shared: Arc<Shared>,
}

impl ClusterStateMachine {
    pub fn open(dir: &Path, opts: SmOptions) -> Result<ClusterStateMachine, OpenError> {
        let snaps = SnapshotStore::open(dir)?;
        let mut cfg = opts.engine;
        cfg.journal = false;
        let restored = restore_current(&snaps, &cfg, &opts.sys, opts.node_id).map_err(|e| {
            let id = snaps.current_meta().map(|m| m.snapshot_id.clone());
            match e.kind() {
                io::ErrorKind::InvalidData => {
                    OpenError::Corrupt(format!("snapshot {}: {e}", id.unwrap_or_default()))
                }
                _ => OpenError::Io(e),
            }
        })?;
        let core = match restored {
            Some((meta, engine, sm_meta)) => Core {
                engine,
                meta: sm_meta,
                last_applied: meta.last_log_id,
                membership: meta.last_membership,
            },
            None => Core {
                engine: new_engine(0, cfg.clone(), (opts.sys)(), opts.node_id),
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
            max_snapshot_bytes: DEFAULT_MAX_SNAPSHOT_BYTES,
        })
    }

    /// Sets the largest snapshot this node accepts from a leader (default
    /// [`DEFAULT_MAX_SNAPSHOT_BYTES`]); a longer one fails to install. Call
    /// before handing the state machine to openraft.
    pub fn set_max_snapshot_bytes(&mut self, max: u64) {
        self.max_snapshot_bytes = max;
    }

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

    /// Apply one connection input (`Op::Conn`, or an item of `Op::Batch`)
    /// at `now`; returns whether it was applied (not a duplicate).
    fn apply_conn(
        &mut self,
        node: NodeId,
        now: Nanos,
        seq: u64,
        input: EngineInput,
        out: &mut Outbox,
        events: &mut Vec<Event>,
    ) -> bool {
        let Some(conn) = input.conn() else {
            tracing::warn!("ignoring a connection entry without a connection: {input:?}");
            return false;
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
            return false;
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
        self.engine.apply_input(now, input, out);
        self.route(node, out, events);
        if disconnect && owner == node {
            events.push(Event::Closed(conn));
        }
        true
    }

    fn apply_request(&mut self, sh: &Shared, req: Request, events: &mut Vec<Event>) -> bool {
        let now = req.now.max(self.meta.last_now);
        self.meta.last_now = now;
        if !self.meta.started {
            self.engine = new_engine(now, sh.cfg.clone(), (sh.sys)(), sh.node_id);
            self.meta.started = true;
        }
        let node = sh.node_id;
        let mut out = Outbox::new();
        match req.op {
            Op::Conn { seq, input } => {
                return !self.apply_conn(node, now, seq, input, &mut out, events);
            }
            Op::Batch(items) => {
                let mut any = false;
                for (seq, input) in items {
                    any |= self.apply_conn(node, now, seq, input, &mut out, events);
                }
                return !any;
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
            Op::DropNode {
                node: n,
                up_to_local,
            } => {
                let conns: Vec<ConnId> = self
                    .engine
                    .conn_ids()
                    .into_iter()
                    .filter(|&c| owner_of(c) == n && local_of(c) <= up_to_local)
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

    pub fn next_deadline(&self) -> Option<Nanos> {
        self.shared.info.borrow().next_deadline
    }

    pub fn subscribe(&self) -> watch::Receiver<AppliedInfo> {
        self.shared.info.subscribe()
    }

    pub fn snapshot_limited(&self, now: Nanos, max_tubes: usize) -> Option<bstk_engine::Snapshot> {
        self.core()
            .map(|c| c.engine.snapshot_limited(now, max_tubes))
    }

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

    pub fn membership(&self) -> Option<Membership> {
        self.core().map(|c| c.membership.clone())
    }

    pub fn export_state(&self) -> Option<EngineState> {
        self.core().map(|c| c.engine.export_state())
    }

    #[cfg(test)]
    pub(crate) fn meta(&self) -> SmMeta {
        self.core().map(|c| c.meta.clone()).unwrap_or_default()
    }
}

impl RaftSnapshotBuilder<TypeConfig> for ClusterStateMachine {
    /// Encodes the state straight into a temporary file in the snapshot
    /// directory while holding the state lock (docs/DESIGN.md §8,
    /// "Streamed snapshots"); syncing and renaming happen after it is
    /// released.
    async fn build_snapshot(&mut self) -> SResult<Snapshot<TypeConfig>> {
        let started = Instant::now();
        let (mut f, temp) = self
            .snaps()?
            .temp_file()
            .map_err(|e| snap_err(None, ErrorVerb::Write, e))?;
        let (encoded, last_applied, membership, locked) = {
            let c = self.shared.lock()?;
            let locked = Instant::now();
            let encoded = write_payload(&f, &c.meta, &c.engine);
            (
                encoded,
                c.last_applied,
                c.membership.clone(),
                locked.elapsed(),
            )
        };
        let (payload_len, payload_crc) =
            encoded.map_err(|e| snap_err(None, ErrorVerb::Write, e))?;
        let mut snaps = self.snaps()?;
        let last = last_applied.map_or_else(|| "none".to_string(), |l| l.to_string());
        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership: membership,
            snapshot_id: snaps.new_id(&last),
        };
        let werr = |e| snap_err(Some(&meta), ErrorVerb::Write, e);
        // A reader of the file as written, also when the build is dropped
        // for being older than the current snapshot (openraft only reads
        // the meta of a built snapshot).
        let reader = File::open(temp.path()).map_err(werr)?;
        let stored = snaps
            .commit(&mut f, temp, &meta, payload_len, payload_crc, false)
            .map_err(werr)?;
        drop(snaps);
        tracing::info!(
            "built snapshot {} ({payload_len} bytes){}: state locked for {:.1} ms, {:.1} ms in all",
            meta.snapshot_id,
            if stored {
                ""
            } else {
                ", older than the current one, dropped"
            },
            locked.as_secs_f64() * 1e3,
            started.elapsed().as_secs_f64() * 1e3
        );
        Ok(Snapshot {
            meta,
            snapshot: Box::new(
                SnapshotFile::reader(reader, HEADER_LEN, payload_len, None)
                    .map_err(|e| snap_err(None, ErrorVerb::Read, e))?,
            ),
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

    async fn begin_receiving_snapshot(&mut self) -> SResult<Box<SnapshotFile>> {
        let (f, temp) = self
            .snaps()?
            .temp_file()
            .map_err(|e| snap_err(None, ErrorVerb::Write, e))?;
        Ok(Box::new(SnapshotFile::receiver(
            f,
            temp,
            HEADER_LEN,
            self.max_snapshot_bytes,
        )))
    }

    /// Decodes and validates the received file before it becomes the
    /// current snapshot; a rejected one is removed with its temporary file.
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<SnapshotFile>,
    ) -> SResult<()> {
        let rerr = |e| snap_err(Some(meta), ErrorVerb::Read, e);
        let werr = |e| snap_err(Some(meta), ErrorVerb::Write, e);
        let mut p = snapshot.into_payload().await.map_err(rerr)?;
        let (mut file, temp) = match p.temp.take() {
            Some(temp) => (p.file, temp),
            // A complete snapshot, not a received one (only openraft's
            // storage test suite does this): store a copy.
            None => {
                let (mut f, temp) = self.snaps()?.temp_file().map_err(werr)?;
                p.file.seek(SeekFrom::Start(p.base)).map_err(rerr)?;
                let n = io::copy(&mut (&p.file).take(p.len), &mut f).map_err(werr)?;
                if n != p.len {
                    return Err(rerr(invalid("snapshot file is shorter than its payload")));
                }
                (f, temp)
            }
        };
        file.seek(SeekFrom::Start(HEADER_LEN)).map_err(rerr)?;
        let (engine, sm_meta, crc) = restore_from(
            &mut file,
            p.len,
            0,
            &self.shared.cfg,
            &self.shared.sys,
            self.shared.node_id,
        )
        .map_err(rerr)?;
        self.snaps()?
            .commit(&mut file, temp, meta, p.len, crc, true)
            .map_err(werr)?;
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
        let opened = snaps.open_current().and_then(|c| {
            c.map(|(meta, f, layout)| {
                SnapshotFile::reader(
                    f,
                    layout.payload_off,
                    layout.payload_len,
                    Some(layout.check()),
                )
                .map(|data| Snapshot {
                    meta,
                    snapshot: Box::new(data),
                })
            })
            .transpose()
        });
        opened.map_err(|e| snap_err(snaps.current_meta(), ErrorVerb::Read, e))
    }
}
