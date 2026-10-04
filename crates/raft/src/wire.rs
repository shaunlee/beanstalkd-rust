//! Wire protocol of the cluster port (docs/DESIGN.md §8).
//!
//! A frame is a `u32` big-endian payload length followed by a postcard
//! message, in both directions. Frames above the configured maximum close the
//! connection, and nothing larger is ever allocated. The dialer sends
//! [`ClientMsg::Hello`] first; the listener answers with [`ServerMsg::Hello`]
//! and then serves [`ClientMsg::Request`]s, each answered by a
//! [`ServerMsg::Response`] with the same request id. Remote failures are
//! values ([`WireError`]), never a dropped connection.
//!
//! An operator tool sends [`ClientMsg::AdminHello`] instead (protocol version
//! 4): the connection then carries only [`ClientMsg::Admin`] requests, each
//! answered by a [`ServerMsg::Admin`]; a peer connection carries no admin
//! request and an admin connection nothing else (docs/DESIGN.md §8 "Cluster
//! protocol v4 and the admin channel").
//!
//! openraft's own error types are not sent as is: `StorageError` embeds a
//! recursive `AnyError` chain that could exhaust the receiver's stack while
//! decoding. [`WireError`] is flat.
//!
//! Decoding is bounded before anything is built, for collections that a few
//! bytes each could expand into large allocations: AppendEntries entries
//! ([`MAX_APPEND_ENTRIES`]), forward items ([`MAX_FORWARD_ITEMS`]), `Op::Batch`
//! items ([`MAX_BATCH_ITEMS`]), membership node sets ([`MAX_MEMBERS`],
//! [`MAX_JOINT_CONFIGS`]) and snapshot meta text ([`MAX_SNAPSHOT_ID_LEN`],
//! [`MAX_NODE_ADDR_LEN`]). A request beyond a limit is a decode error, which
//! closes the connection. Status probes have a fixed size and need no bound;
//! the membership in a [`RpcResponse::StatusEx`] or an [`AdminResponse`] and
//! the ids and addresses of an [`AdminRequest`] are bounded the same way.

use std::fmt;
use std::io;

use openraft::error::{Fatal, InstallSnapshotError, RaftError, SnapshotMismatch};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{ErrorSubject, ErrorVerb, LogId, StorageError, StorageIOError, Vote};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::forward::{ControlRequest, ControlResponse};
use crate::status::{NodeStatus, NodeStatusEx};
use crate::{ForwardRequest, ForwardResponse, NodeId, TypeConfig};

/// Version of this wire protocol, carried in the hellos.
///
/// - 1: P3-T3 (Raft RPCs and input forwarding).
/// - 2: P3-T4: the hellos carry `max_job_size` (peers with a different
///   `-z` are rejected), and control requests ([`RpcRequest::Control`]).
/// - 3: P3-FC: status probes ([`RpcRequest::Status`]), answered from the
///   log store even before Raft runs (safe rejoin, `--cluster-init`).
/// - 4: P6-T2: [`RpcRequest::StatusEx`] (status plus membership) and the
///   admin channel ([`ClientMsg::AdminHello`], [`AdminRequest`]). Appended
///   variants only; nodes still require an exact version (0.5.0 was never
///   released, so no deployed cluster needs negotiation). P6-T3 appended
///   [`ClientMsg::ProbeHello`] (status probes from nodes that are not
///   members yet) within the same unreleased version.
pub const PROTOCOL_VERSION: u32 = 4;

pub const HEADER_LEN: usize = 4;

/// Default maximum payload size of one frame (32 MiB). It must exceed the
/// largest single log entry (a job body up to `-z` plus overhead) and
/// openraft's `snapshot_max_chunk_size`.
pub const DEFAULT_MAX_FRAME: usize = 32 << 20;

/// Most entries accepted in one AppendEntries request. openraft sends at
/// most `Config::max_payload_entries` (300 by default; the server keeps the
/// default), so this only bounds what a faulty peer can make us allocate.
pub const MAX_APPEND_ENTRIES: usize = 4096;

/// Most items accepted in one forward request (the server batches at most
/// 1024).
pub const MAX_FORWARD_ITEMS: usize = 4096;

/// Most items accepted in one `Op::Batch` (in an AppendEntries entry or
/// a control request): exactly what the server proposes at most
/// ([`crate::MAX_PROPOSAL_ITEMS`]), so one AppendEntries holds at most
/// [`MAX_APPEND_ENTRIES`] × this many inputs.
pub const MAX_BATCH_ITEMS: usize = crate::MAX_PROPOSAL_ITEMS;

/// Most node ids in one membership config, and nodes in one membership
/// (clusters have 1, 3 or 5 nodes).
pub const MAX_MEMBERS: usize = 256;

/// Most configs in one (joint) membership; openraft uses at most two.
pub const MAX_JOINT_CONFIGS: usize = 4;

/// Longest snapshot id accepted from a peer, in bytes. The meta of an
/// installed snapshot is stored with it and must stay below the store's
/// limit for reading a snapshot file back (`MAX_META_LEN`); ours are
/// `<last log id>-<seq>`.
pub const MAX_SNAPSHOT_ID_LEN: usize = 256;

/// Longest node address accepted from a peer in a membership, in bytes
/// (also bounds the meta of a snapshot: [`MAX_MEMBERS`] of them stay below
/// the store's `MAX_META_LEN`).
pub const MAX_NODE_ADDR_LEN: usize = 1024;

/// Longest refusal reason accepted in an [`AdminResponse`], in bytes.
pub const MAX_ADMIN_REASON_LEN: usize = 1024;

/// Largest admin request frame a listener reads (the biggest, a `Promote`
/// of [`MAX_MEMBERS`] ids, is about 1 KiB). Answers may be larger: a
/// membership of [`MAX_MEMBERS`] nodes with the longest addresses is about
/// 270 KiB.
pub const ADMIN_MAX_REQUEST_FRAME: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hello {
    pub version: u32,
    pub from: NodeId,
    pub to: NodeId,
    /// The dialer's `-z`. Every node must use the same value (the engine
    /// replies `JOB_TOO_BIG` by it), so the listener rejects a mismatch.
    pub max_job_size: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServerHello {
    Accepted {
        version: u32,
        node_id: NodeId,
        /// The listener's `-z` (equal to the dialer's, or the hello would
        /// have been rejected); the dialer checks it too.
        max_job_size: u32,
    },
    Rejected {
        reason: String,
    },
}

/// The hello of an operator tool (protocol version 4). It claims no node
/// id: under mTLS the client certificate must carry exactly the SAN
/// [`crate::tls::ADMIN_DNS_NAME`]; in plaintext mode the source must be a
/// loopback address. Answered by a [`ServerHello`] like a peer's hello.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminHello {
    pub version: u32,
    /// The node the tool means to reach (`None`: whichever answers); a
    /// mismatch is rejected like a misaddressed peer hello.
    pub to: Option<NodeId>,
}

/// Variants are only ever appended: their indexes are the encoding.
#[derive(Debug, Serialize, Deserialize)]
pub enum ClientMsg {
    Hello(Hello),
    Request {
        id: u64,
        body: RpcRequest,
    },
    /// Version 4.
    AdminHello(AdminHello),
    /// Version 4: on an admin connection only.
    Admin {
        id: u64,
        body: AdminRequest,
    },
    /// Version 4 (P6-T3): a startup probe from a node that may not be a
    /// member yet. Identity is checked as for [`ClientMsg::Hello`], the
    /// membership is not; the connection then carries only status probes
    /// ([`RpcRequest::Status`], [`RpcRequest::StatusEx`]).
    ProbeHello(Hello),
}

/// Variants are only ever appended: their indexes are the encoding.
#[derive(Debug, Serialize, Deserialize)]
pub enum ServerMsg {
    Hello(ServerHello),
    Response {
        id: u64,
        body: RpcResponse,
    },
    /// Version 4: the answer to [`ClientMsg::Admin`].
    Admin {
        id: u64,
        body: AdminResponse,
    },
}

/// An operator request (protocol version 4). Each change carries `expect`,
/// the membership log id the operator saw: the node refuses the change with
/// [`AdminResponse::Conflict`] if its membership is another one
/// (compare-and-set), so two operators cannot both act on the same view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdminRequest {
    /// The node's status and membership view ([`AdminResponse::Membership`]).
    Membership,
    AddLearner {
        id: NodeId,
        #[serde(deserialize_with = "bounded::node_addr")]
        addr: String,
        expect: Option<LogId<NodeId>>,
    },
    Promote {
        #[serde(deserialize_with = "bounded::node_ids")]
        ids: std::collections::BTreeSet<NodeId>,
        expect: Option<LogId<NodeId>>,
    },
    Remove {
        id: NodeId,
        expect: Option<LogId<NodeId>>,
    },
    SetAddr {
        id: NodeId,
        #[serde(deserialize_with = "bounded::node_addr")]
        addr: String,
        expect: Option<LogId<NodeId>>,
    },
}

impl AdminRequest {
    /// Whether the request asks for a membership change.
    pub fn is_change(&self) -> bool {
        !matches!(self, AdminRequest::Membership)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdminResponse {
    Membership(Box<NodeStatusEx>),
    /// The leader accepted the change and runs it in the background; poll
    /// [`AdminRequest::Membership`] for the outcome.
    Started,
    /// The change is complete (or there was nothing to do); `log_id` is the
    /// resulting membership's.
    Done {
        log_id: Option<LogId<NodeId>>,
    },
    /// Changes are made by the leader: ask it.
    NotLeader {
        leader: Option<NodeId>,
        #[serde(deserialize_with = "bounded::opt_node_addr")]
        addr: Option<String>,
    },
    /// `expect` is not the current membership's log id.
    Conflict {
        current: Option<LogId<NodeId>>,
    },
    /// A guardrail refused the change.
    Refused {
        #[serde(deserialize_with = "bounded::admin_reason")]
        reason: String,
    },
    /// This node does not implement the request.
    Unsupported,
}

/// A request. The same encoding as the derived one; decoding applies the
/// limits of the module docs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RpcRequest {
    AppendEntries(#[serde(deserialize_with = "bounded::append")] AppendEntriesRequest<TypeConfig>),
    Vote(VoteRequest<NodeId>),
    InstallSnapshot(
        #[serde(deserialize_with = "bounded::install")] InstallSnapshotRequest<TypeConfig>,
    ),
    Forward(#[serde(deserialize_with = "bounded::forward")] ForwardRequest),
    Control(#[serde(deserialize_with = "bounded::control")] ControlRequest),
    /// The peer's durable Raft state (version 3), answered from its log
    /// store whether or not its Raft is running (see [`crate::status`]).
    Status,
    /// [`RpcRequest::Status`] plus the node's membership view (version 4),
    /// served in the same situations.
    StatusEx,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcResponse {
    AppendEntries(Result<AppendEntriesResponse<NodeId>, WireError>),
    Vote(Result<VoteResponse<NodeId>, WireError>),
    InstallSnapshot(Result<InstallSnapshotResponse<NodeId>, WireError>),
    Forward(Result<ForwardResponse, WireError>),
    Control(Result<ControlResponse, WireError>),
    Status {
        vote: Option<Vote<NodeId>>,
        last_log_id: Option<LogId<NodeId>>,
        committed: Option<LogId<NodeId>>,
        has_state: bool,
    },
    StatusEx(Result<Box<NodeStatusEx>, WireError>),
}

impl RpcResponse {
    pub fn status(s: NodeStatus) -> RpcResponse {
        RpcResponse::Status {
            vote: s.vote,
            last_log_id: s.last_log_id,
            committed: s.committed,
            has_state: s.has_state,
        }
    }

    pub fn into_status(self) -> Option<NodeStatus> {
        match self {
            RpcResponse::Status {
                vote,
                last_log_id,
                committed,
                has_state,
            } => Some(NodeStatus {
                vote,
                last_log_id,
                committed,
                has_state,
            }),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WireError {
    Fatal(WireFatal),
    /// `InstallSnapshotError::SnapshotMismatch`: the sender restarts the
    /// snapshot from offset 0.
    SnapshotMismatch(SnapshotMismatch),
    /// The listener refused the request (for example a forward whose
    /// `from` is not the authenticated peer).
    Rejected(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WireFatal {
    Stopped,
    Panicked,
    Storage(String),
}

impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WireError::Fatal(WireFatal::Stopped) => f.write_str("remote raft stopped"),
            WireError::Fatal(WireFatal::Panicked) => f.write_str("remote raft panicked"),
            WireError::Fatal(WireFatal::Storage(m)) => write!(f, "remote storage error: {m}"),
            WireError::SnapshotMismatch(m) => write!(f, "snapshot mismatch: {m}"),
            WireError::Rejected(m) => write!(f, "rejected: {m}"),
        }
    }
}

impl std::error::Error for WireError {}

impl WireFatal {
    pub fn from_fatal(f: &Fatal<NodeId>) -> Self {
        match f {
            Fatal::Stopped => WireFatal::Stopped,
            Fatal::Panicked => WireFatal::Panicked,
            Fatal::StorageError(e) => WireFatal::Storage(e.to_string()),
        }
    }

    pub fn into_fatal(self) -> Fatal<NodeId> {
        match self {
            WireFatal::Stopped => Fatal::Stopped,
            WireFatal::Panicked => Fatal::Panicked,
            WireFatal::Storage(m) => Fatal::StorageError(StorageError::from(StorageIOError::new(
                ErrorSubject::Store,
                ErrorVerb::Read,
                &io::Error::other(m),
            ))),
        }
    }
}

impl WireError {
    pub fn from_raft(e: &RaftError<NodeId>) -> Self {
        match e {
            RaftError::APIError(never) => match *never {},
            RaftError::Fatal(f) => WireError::Fatal(WireFatal::from_fatal(f)),
        }
    }

    pub fn from_snapshot(e: &RaftError<NodeId, InstallSnapshotError>) -> Self {
        match e {
            RaftError::APIError(InstallSnapshotError::SnapshotMismatch(m)) => {
                WireError::SnapshotMismatch(m.clone())
            }
            RaftError::Fatal(f) => WireError::Fatal(WireFatal::from_fatal(f)),
        }
    }
}

/// Decoding with limits: mirrors of openraft's request types (same fields
/// in the same order, hence the same postcard encoding) whose collections
/// are decoded by bounded visitors.
mod bounded {
    use std::collections::{BTreeMap, BTreeSet};
    use std::fmt;
    use std::marker::PhantomData;

    use bstk_engine::{ConnId, EngineInput, Nanos};
    use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest};
    use openraft::{
        BasicNode, Entry, EntryPayload, LogId, Membership, SnapshotMeta, StoredMembership, Vote,
    };
    use serde::Deserialize;
    use serde::de::{self, Deserializer, MapAccess, SeqAccess, Visitor};

    use super::{
        MAX_APPEND_ENTRIES, MAX_BATCH_ITEMS, MAX_FORWARD_ITEMS, MAX_JOINT_CONFIGS, MAX_MEMBERS,
        MAX_NODE_ADDR_LEN, MAX_SNAPSHOT_ID_LEN,
    };
    use crate::forward::ControlRequest;
    use crate::{ForwardRequest, NodeId, Op, Request, TypeConfig};

    /// A sequence of at most `max` elements, rejected as soon as its
    /// announced length (or its actual one) exceeds `max`.
    struct SeqVisitor<T> {
        max: usize,
        _t: PhantomData<T>,
    }

    impl<'de, T: Deserialize<'de>> Visitor<'de> for SeqVisitor<T> {
        type Value = Vec<T>;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "a sequence of at most {} elements", self.max)
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<T>, A::Error> {
            let hint = seq.size_hint().unwrap_or(0);
            if hint > self.max {
                return Err(de::Error::invalid_length(hint, &self));
            }
            let mut v = Vec::with_capacity(hint);
            while let Some(x) = seq.next_element()? {
                if v.len() >= self.max {
                    return Err(de::Error::invalid_length(v.len() + 1, &self));
                }
                v.push(x);
            }
            Ok(v)
        }
    }

    fn seq<'de, D: Deserializer<'de>, T: Deserialize<'de>>(
        d: D,
        max: usize,
    ) -> Result<Vec<T>, D::Error> {
        d.deserialize_seq(SeqVisitor {
            max,
            _t: PhantomData,
        })
    }

    struct MapVisitor<K, V> {
        max: usize,
        _kv: PhantomData<(K, V)>,
    }

    impl<'de, K: Deserialize<'de> + Ord, V: Deserialize<'de>> Visitor<'de> for MapVisitor<K, V> {
        type Value = BTreeMap<K, V>;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "a map of at most {} entries", self.max)
        }

        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
            let hint = map.size_hint().unwrap_or(0);
            if hint > self.max {
                return Err(de::Error::invalid_length(hint, &self));
            }
            let mut m = BTreeMap::new();
            let mut n = 0usize;
            while let Some((k, v)) = map.next_entry()? {
                n += 1;
                if n > self.max {
                    return Err(de::Error::invalid_length(n, &self));
                }
                m.insert(k, v);
            }
            Ok(m)
        }
    }

    struct StrVisitor {
        max: usize,
    }

    impl Visitor<'_> for StrVisitor {
        type Value = String;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "a string of at most {} bytes", self.max)
        }

        fn visit_str<E: de::Error>(self, v: &str) -> Result<String, E> {
            if v.len() > self.max {
                return Err(de::Error::invalid_length(v.len(), &self));
            }
            Ok(v.to_owned())
        }
    }

    pub(super) fn admin_reason<'de, D: Deserializer<'de>>(d: D) -> Result<String, D::Error> {
        d.deserialize_str(StrVisitor {
            max: super::MAX_ADMIN_REASON_LEN,
        })
    }

    struct OptAddrVisitor;

    impl<'de> Visitor<'de> for OptAddrVisitor {
        type Value = Option<String>;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "an optional string of at most {MAX_NODE_ADDR_LEN} bytes")
        }

        fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
            node_addr(d).map(Some)
        }
    }

    pub(super) fn opt_node_addr<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<Option<String>, D::Error> {
        d.deserialize_option(OptAddrVisitor)
    }

    pub(super) fn node_ids<'de, D: Deserializer<'de>>(d: D) -> Result<BTreeSet<NodeId>, D::Error> {
        let ids: Vec<NodeId> = seq(d, MAX_MEMBERS)?;
        Ok(ids.into_iter().collect())
    }

    pub(super) fn voter_sets<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<Vec<BTreeSet<NodeId>>, D::Error> {
        Ok(configs(d)?.into_iter().map(|c| c.0).collect())
    }

    struct Addr(String);

    impl<'de> Deserialize<'de> for Addr {
        fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            node_addr(d).map(Addr)
        }
    }

    pub(super) fn node_addrs<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<BTreeMap<NodeId, String>, D::Error> {
        let m: BTreeMap<NodeId, Addr> = d.deserialize_map(MapVisitor {
            max: MAX_MEMBERS,
            _kv: PhantomData,
        })?;
        Ok(m.into_iter().map(|(id, a)| (id, a.0)).collect())
    }

    fn snapshot_id<'de, D: Deserializer<'de>>(d: D) -> Result<String, D::Error> {
        d.deserialize_str(StrVisitor {
            max: MAX_SNAPSHOT_ID_LEN,
        })
    }

    #[derive(Deserialize)]
    struct NodeWire {
        #[serde(deserialize_with = "node_addr")]
        addr: String,
    }

    pub(super) fn node_addr<'de, D: Deserializer<'de>>(d: D) -> Result<String, D::Error> {
        d.deserialize_str(StrVisitor {
            max: MAX_NODE_ADDR_LEN,
        })
    }

    fn entries<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<EntryWire>, D::Error> {
        seq(d, MAX_APPEND_ENTRIES)
    }

    fn items<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<(ConnId, u64, EngineInput)>, D::Error> {
        seq(d, MAX_FORWARD_ITEMS)
    }

    fn batch<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<(u64, EngineInput)>, D::Error> {
        seq(d, MAX_BATCH_ITEMS)
    }

    /// Mirror of [`Op`] (same variants in the same order).
    #[derive(Deserialize)]
    enum OpWire {
        Conn { seq: u64, input: EngineInput },
        Tick,
        SetDraining(bool),
        DropNode { node: NodeId, up_to_local: u64 },
        Batch(#[serde(deserialize_with = "batch")] Vec<(u64, EngineInput)>),
    }

    impl From<OpWire> for Op {
        fn from(o: OpWire) -> Self {
            match o {
                OpWire::Conn { seq, input } => Op::Conn { seq, input },
                OpWire::Tick => Op::Tick,
                OpWire::SetDraining(on) => Op::SetDraining(on),
                OpWire::DropNode { node, up_to_local } => Op::DropNode { node, up_to_local },
                OpWire::Batch(items) => Op::Batch(items),
            }
        }
    }

    #[derive(Deserialize)]
    struct RequestWire {
        now: Nanos,
        op: OpWire,
    }

    impl From<RequestWire> for Request {
        fn from(r: RequestWire) -> Self {
            Request {
                now: r.now,
                op: r.op.into(),
            }
        }
    }

    #[derive(Deserialize)]
    struct ControlWire {
        from: NodeId,
        op: OpWire,
    }

    pub(super) fn control<'de, D: Deserializer<'de>>(d: D) -> Result<ControlRequest, D::Error> {
        let c = ControlWire::deserialize(d)?;
        Ok(ControlRequest {
            from: c.from,
            op: c.op.into(),
        })
    }

    struct Config(BTreeSet<NodeId>);

    impl<'de> Deserialize<'de> for Config {
        fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            let ids: Vec<NodeId> = seq(d, MAX_MEMBERS)?;
            Ok(Config(ids.into_iter().collect()))
        }
    }

    fn configs<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<Config>, D::Error> {
        seq(d, MAX_JOINT_CONFIGS)
    }

    fn nodes<'de, D: Deserializer<'de>>(d: D) -> Result<BTreeMap<NodeId, BasicNode>, D::Error> {
        let m: BTreeMap<NodeId, NodeWire> = d.deserialize_map(MapVisitor {
            max: MAX_MEMBERS,
            _kv: PhantomData,
        })?;
        Ok(m.into_iter()
            .map(|(id, n)| (id, BasicNode::new(n.addr)))
            .collect())
    }

    #[derive(Deserialize)]
    struct MembershipWire {
        #[serde(deserialize_with = "configs")]
        configs: Vec<Config>,
        #[serde(deserialize_with = "nodes")]
        nodes: BTreeMap<NodeId, BasicNode>,
    }

    impl From<MembershipWire> for Membership<NodeId, BasicNode> {
        fn from(m: MembershipWire) -> Self {
            Membership::new(m.configs.into_iter().map(|c| c.0).collect(), m.nodes)
        }
    }

    #[derive(Deserialize)]
    enum PayloadWire {
        Blank,
        Normal(RequestWire),
        Membership(MembershipWire),
    }

    #[derive(Deserialize)]
    struct EntryWire {
        log_id: LogId<NodeId>,
        payload: PayloadWire,
    }

    impl From<EntryWire> for Entry<TypeConfig> {
        fn from(e: EntryWire) -> Self {
            Entry {
                log_id: e.log_id,
                payload: match e.payload {
                    PayloadWire::Blank => EntryPayload::Blank,
                    PayloadWire::Normal(r) => EntryPayload::Normal(r.into()),
                    PayloadWire::Membership(m) => EntryPayload::Membership(m.into()),
                },
            }
        }
    }

    #[derive(Deserialize)]
    struct AppendWire {
        vote: Vote<NodeId>,
        prev_log_id: Option<LogId<NodeId>>,
        #[serde(deserialize_with = "entries")]
        entries: Vec<EntryWire>,
        leader_commit: Option<LogId<NodeId>>,
    }

    pub(super) fn append<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<AppendEntriesRequest<TypeConfig>, D::Error> {
        let a = AppendWire::deserialize(d)?;
        Ok(AppendEntriesRequest {
            vote: a.vote,
            prev_log_id: a.prev_log_id,
            entries: a.entries.into_iter().map(Entry::from).collect(),
            leader_commit: a.leader_commit,
        })
    }

    #[derive(Deserialize)]
    struct StoredWire {
        log_id: Option<LogId<NodeId>>,
        membership: MembershipWire,
    }

    #[derive(Deserialize)]
    struct MetaWire {
        last_log_id: Option<LogId<NodeId>>,
        last_membership: StoredWire,
        #[serde(deserialize_with = "snapshot_id")]
        snapshot_id: String,
    }

    #[derive(Deserialize)]
    struct InstallWire {
        vote: Vote<NodeId>,
        meta: MetaWire,
        offset: u64,
        data: Vec<u8>,
        done: bool,
    }

    pub(super) fn install<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<InstallSnapshotRequest<TypeConfig>, D::Error> {
        let i = InstallWire::deserialize(d)?;
        let m = i.meta;
        Ok(InstallSnapshotRequest {
            vote: i.vote,
            meta: SnapshotMeta {
                last_log_id: m.last_log_id,
                last_membership: StoredMembership::new(
                    m.last_membership.log_id,
                    m.last_membership.membership.into(),
                ),
                snapshot_id: m.snapshot_id,
            },
            offset: i.offset,
            data: i.data,
            done: i.done,
        })
    }

    #[derive(Deserialize)]
    struct ForwardWire {
        from: NodeId,
        #[serde(deserialize_with = "items")]
        items: Vec<(ConnId, u64, EngineInput)>,
    }

    pub(super) fn forward<'de, D: Deserializer<'de>>(d: D) -> Result<ForwardRequest, D::Error> {
        let f = ForwardWire::deserialize(d)?;
        Ok(ForwardRequest {
            from: f.from,
            items: f.items,
        })
    }
}

/// Bounded decoding of [`crate::status::MembershipView`]'s voter sets.
pub(crate) fn bounded_voter_sets<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Vec<std::collections::BTreeSet<NodeId>>, D::Error> {
    bounded::voter_sets(d)
}

/// Bounded decoding of [`crate::status::MembershipView`]'s node addresses.
pub(crate) fn bounded_node_addrs<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<std::collections::BTreeMap<NodeId, String>, D::Error> {
    bounded::node_addrs(d)
}

/// Longest peer-supplied text kept by [`sanitize`], in characters.
pub const MAX_PEER_TEXT: usize = 200;

/// A string received from a peer (a rejection reason, a remote error),
/// made safe to log or to embed in local errors: at most
/// [`MAX_PEER_TEXT`] characters, control and non-printable characters
/// escaped (`char::escape_debug`).
pub fn sanitize(s: &str) -> String {
    let mut out: String = s
        .chars()
        .take(MAX_PEER_TEXT)
        .flat_map(char::escape_debug)
        .collect();
    if s.chars().nth(MAX_PEER_TEXT).is_some() {
        out.push_str("...");
    }
    out
}

#[derive(Debug)]
pub enum FrameError {
    Io(io::Error),
    TooLarge { len: usize, max: usize },
    Truncated,
    Decode(postcard::Error),
    Encode(postcard::Error),
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameError::Io(e) => write!(f, "i/o error: {e}"),
            FrameError::TooLarge { len, max } => {
                write!(f, "frame of {len} bytes exceeds the maximum of {max}")
            }
            FrameError::Truncated => f.write_str("stream ended inside a frame"),
            FrameError::Decode(e) => write!(f, "invalid frame: {e}"),
            FrameError::Encode(e) => write!(f, "cannot encode frame: {e}"),
        }
    }
}

impl std::error::Error for FrameError {}

impl From<io::Error> for FrameError {
    fn from(e: io::Error) -> Self {
        FrameError::Io(e)
    }
}

pub fn encode<T: Serialize>(msg: &T, max_frame: usize) -> Result<Vec<u8>, FrameError> {
    let buf = postcard::to_extend(msg, vec![0u8; HEADER_LEN]).map_err(FrameError::Encode)?;
    let len = buf.len() - HEADER_LEN;
    if len > max_frame {
        return Err(FrameError::TooLarge {
            len,
            max: max_frame,
        });
    }
    let mut buf = buf;
    let header = u32::try_from(len)
        .map_err(|_| FrameError::TooLarge {
            len,
            max: max_frame,
        })?
        .to_be_bytes();
    buf[..HEADER_LEN].copy_from_slice(&header);
    Ok(buf)
}

/// Decodes one frame from the front of `buf`: `Ok(None)` if `buf` does not
/// hold a complete frame yet, otherwise the message and the bytes consumed.
/// An oversized length is rejected before its payload arrives.
pub fn decode<T: DeserializeOwned>(
    buf: &[u8],
    max_frame: usize,
) -> Result<Option<(T, usize)>, FrameError> {
    let Some(header) = buf.get(..HEADER_LEN) else {
        return Ok(None);
    };
    let len = frame_len(header, max_frame)?;
    let Some(payload) = buf.get(HEADER_LEN..HEADER_LEN + len) else {
        return Ok(None);
    };
    let msg = postcard::from_bytes(payload).map_err(FrameError::Decode)?;
    Ok(Some((msg, HEADER_LEN + len)))
}

fn frame_len(header: &[u8], max_frame: usize) -> Result<usize, FrameError> {
    let mut h = [0u8; HEADER_LEN];
    h.copy_from_slice(header);
    let len = u32::from_be_bytes(h) as usize;
    if len > max_frame {
        return Err(FrameError::TooLarge {
            len,
            max: max_frame,
        });
    }
    Ok(len)
}

/// Reads one frame. `Ok(None)` on a clean end of stream before the first
/// header byte. The payload buffer grows with the bytes actually received,
/// so a peer announcing a large frame cannot make us allocate it up front.
pub async fn read_frame<R, T>(r: &mut R, max_frame: usize) -> Result<Option<T>, FrameError>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let mut header = [0u8; HEADER_LEN];
    let mut got = 0;
    while got < HEADER_LEN {
        let n = r.read(&mut header[got..]).await?;
        if n == 0 {
            return if got == 0 {
                Ok(None)
            } else {
                Err(FrameError::Truncated)
            };
        }
        got += n;
    }
    let len = frame_len(&header, max_frame)?;
    let mut payload = Vec::with_capacity(len.min(64 * 1024));
    let n = (&mut *r).take(len as u64).read_to_end(&mut payload).await?;
    if n < len {
        return Err(FrameError::Truncated);
    }
    postcard::from_bytes(&payload)
        .map(Some)
        .map_err(FrameError::Decode)
}

pub async fn write_frame<W>(w: &mut W, frame: &[u8]) -> Result<(), FrameError>
where
    W: AsyncWrite + Unpin,
{
    w.write_all(frame).await?;
    w.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Op, Request};
    use bstk_engine::EngineInput;
    use openraft::{CommittedLeaderId, Entry, EntryPayload, SnapshotMeta};

    fn sample_append() -> ClientMsg {
        let leader = CommittedLeaderId::new(3, 1);
        ClientMsg::Request {
            id: 7,
            body: RpcRequest::AppendEntries(AppendEntriesRequest {
                vote: Vote::new_committed(3, 1),
                prev_log_id: Some(LogId::new(leader, 10)),
                entries: vec![Entry {
                    log_id: LogId::new(leader, 11),
                    payload: EntryPayload::Normal(Request {
                        now: 42,
                        op: Op::Conn {
                            seq: 1,
                            input: EngineInput::Connect(1 << 48),
                        },
                    }),
                }],
                leader_commit: Some(LogId::new(leader, 9)),
            }),
        }
    }

    #[test]
    fn round_trip_every_message_kind() {
        let msgs = vec![
            ClientMsg::Hello(Hello {
                version: PROTOCOL_VERSION,
                from: 2,
                to: 1,
                max_job_size: 65535,
            }),
            sample_append(),
            ClientMsg::Request {
                id: 8,
                body: RpcRequest::Vote(VoteRequest::new(Vote::new(4, 2), None)),
            },
            ClientMsg::Request {
                id: 9,
                body: RpcRequest::InstallSnapshot(InstallSnapshotRequest {
                    vote: Vote::new_committed(3, 1),
                    meta: SnapshotMeta {
                        last_log_id: None,
                        last_membership: Default::default(),
                        snapshot_id: "s-1".to_string(),
                    },
                    offset: 5,
                    data: vec![1, 2, 3],
                    done: false,
                }),
            },
            ClientMsg::Request {
                id: 10,
                body: RpcRequest::Forward(ForwardRequest {
                    from: 2,
                    items: vec![(2 << 48 | 5, 3, EngineInput::HalfClose(2 << 48 | 5))],
                }),
            },
            ClientMsg::Request {
                id: 11,
                body: RpcRequest::Control(ControlRequest {
                    from: 2,
                    op: Op::DropNode {
                        node: 2,
                        up_to_local: 9,
                    },
                }),
            },
            ClientMsg::Request {
                id: 12,
                body: RpcRequest::Control(ControlRequest {
                    from: 3,
                    op: Op::SetDraining(true),
                }),
            },
            ClientMsg::Request {
                id: 13,
                body: RpcRequest::Status,
            },
        ];
        let mut stream = Vec::new();
        for m in &msgs {
            stream.extend(encode(m, DEFAULT_MAX_FRAME).expect("encode"));
        }
        let mut at = 0;
        for m in &msgs {
            let (got, used): (ClientMsg, usize) = decode(&stream[at..], DEFAULT_MAX_FRAME)
                .expect("decode")
                .expect("complete");
            assert_eq!(format!("{got:?}"), format!("{m:?}"));
            assert_eq!(
                &stream[at..at + used],
                &encode(&got, DEFAULT_MAX_FRAME).expect("re")[..]
            );
            at += used;
        }
        assert_eq!(at, stream.len());

        let responses = vec![
            ServerMsg::Hello(ServerHello::Accepted {
                version: PROTOCOL_VERSION,
                node_id: 1,
                max_job_size: 100,
            }),
            ServerMsg::Hello(ServerHello::Rejected {
                reason: "no".into(),
            }),
            ServerMsg::Response {
                id: 1,
                body: RpcResponse::AppendEntries(Ok(AppendEntriesResponse::Success)),
            },
            ServerMsg::Response {
                id: 2,
                body: RpcResponse::Vote(Err(WireError::Fatal(WireFatal::Storage("x".into())))),
            },
            ServerMsg::Response {
                id: 3,
                body: RpcResponse::InstallSnapshot(Err(WireError::SnapshotMismatch(
                    SnapshotMismatch {
                        expect: openraft::SnapshotSegmentId {
                            id: "a".into(),
                            offset: 0,
                        },
                        got: openraft::SnapshotSegmentId {
                            id: "a".into(),
                            offset: 9,
                        },
                    },
                ))),
            },
            ServerMsg::Response {
                id: 4,
                body: RpcResponse::Forward(Ok(ForwardResponse::NotLeader { leader: Some(3) })),
            },
            ServerMsg::Response {
                id: 5,
                body: RpcResponse::Control(Ok(ControlResponse::Accepted { index: Some(42) })),
            },
            ServerMsg::Response {
                id: 6,
                body: RpcResponse::Control(Ok(ControlResponse::NotLeader { leader: None })),
            },
            ServerMsg::Response {
                id: 7,
                body: RpcResponse::Control(Err(WireError::Rejected("no".into()))),
            },
            ServerMsg::Response {
                id: 8,
                body: RpcResponse::status(NodeStatus {
                    vote: Some(Vote::new_committed(7, 2)),
                    last_log_id: Some(LogId::new(CommittedLeaderId::new(7, 2), 40)),
                    committed: Some(LogId::new(CommittedLeaderId::new(6, 1), 30)),
                    has_state: true,
                }),
            },
            ServerMsg::Response {
                id: 9,
                body: RpcResponse::status(NodeStatus::default()),
            },
        ];
        for m in &responses {
            let frame = encode(m, DEFAULT_MAX_FRAME).expect("encode");
            let (got, used): (ServerMsg, usize) = decode(&frame, DEFAULT_MAX_FRAME)
                .expect("decode")
                .expect("complete");
            assert_eq!(format!("{got:?}"), format!("{m:?}"));
            assert_eq!(used, frame.len());
        }
        let s = NodeStatus {
            vote: Some(Vote::new(3, 1)),
            last_log_id: None,
            committed: None,
            has_state: true,
        };
        let frame = encode(
            &ServerMsg::Response {
                id: 1,
                body: RpcResponse::status(s),
            },
            DEFAULT_MAX_FRAME,
        )
        .expect("encode");
        let (got, _): (ServerMsg, usize) = decode(&frame, DEFAULT_MAX_FRAME)
            .expect("decode")
            .expect("complete");
        let ServerMsg::Response { body, .. } = got else {
            panic!("response expected")
        };
        assert_eq!(body.into_status(), Some(s));
    }

    #[test]
    fn oversize_is_rejected_on_both_sides() {
        let msg = sample_append();
        let frame = encode(&msg, DEFAULT_MAX_FRAME).expect("encode");
        let len = frame.len() - HEADER_LEN;
        assert!(matches!(
            encode(&msg, len - 1),
            Err(FrameError::TooLarge { .. })
        ));
        // The receiver rejects on the header alone.
        assert!(matches!(
            decode::<ClientMsg>(&frame[..HEADER_LEN], len - 1),
            Err(FrameError::TooLarge { .. })
        ));
        let huge = u32::MAX.to_be_bytes();
        assert!(matches!(
            decode::<ClientMsg>(&huge, DEFAULT_MAX_FRAME),
            Err(FrameError::TooLarge { .. })
        ));
    }

    #[test]
    fn truncated_frames_are_incomplete() {
        let frame = encode(&sample_append(), DEFAULT_MAX_FRAME).expect("encode");
        for cut in 0..frame.len() {
            assert!(
                decode::<ClientMsg>(&frame[..cut], DEFAULT_MAX_FRAME)
                    .expect("no error")
                    .is_none()
            );
        }
    }

    #[test]
    fn garbage_is_a_decode_error() {
        let mut frame = vec![0, 0, 0, 3];
        frame.extend([0xff, 0xff, 0xff]);
        assert!(matches!(
            decode::<ClientMsg>(&frame, DEFAULT_MAX_FRAME),
            Err(FrameError::Decode(_))
        ));
        let empty = [0u8, 0, 0, 0];
        assert!(matches!(
            decode::<ServerMsg>(&empty, DEFAULT_MAX_FRAME),
            Err(FrameError::Decode(_))
        ));
    }

    #[tokio::test]
    async fn async_reader_handles_eof_truncation_and_oversize() {
        let frame = encode(&sample_append(), DEFAULT_MAX_FRAME).expect("encode");
        let mut two = frame.clone();
        two.extend(&frame);
        let mut r = two.as_slice();
        for _ in 0..2 {
            let m: Option<ClientMsg> = read_frame(&mut r, DEFAULT_MAX_FRAME).await.expect("read");
            assert_eq!(format!("{m:?}"), format!("{:?}", Some(sample_append())));
        }
        let m: Option<ClientMsg> = read_frame(&mut r, DEFAULT_MAX_FRAME).await.expect("eof");
        assert!(m.is_none());

        for cut in [2, HEADER_LEN + 1, frame.len() - 1] {
            let mut r = &frame[..cut];
            let e = read_frame::<_, ClientMsg>(&mut r, DEFAULT_MAX_FRAME).await;
            assert!(matches!(e, Err(FrameError::Truncated)), "cut {cut}");
        }

        // Oversize announced length: rejected without reading the payload.
        let mut r = &u32::MAX.to_be_bytes()[..];
        let e = read_frame::<_, ClientMsg>(&mut r, DEFAULT_MAX_FRAME).await;
        assert!(matches!(e, Err(FrameError::TooLarge { .. })));

        let mut r = &[0u8, 0, 0, 2, 0xff, 0xff][..];
        let e = read_frame::<_, ClientMsg>(&mut r, DEFAULT_MAX_FRAME).await;
        assert!(matches!(e, Err(FrameError::Decode(_))));
    }

    fn append_with(entries: Vec<Entry<TypeConfig>>) -> ClientMsg {
        ClientMsg::Request {
            id: 1,
            body: RpcRequest::AppendEntries(AppendEntriesRequest {
                vote: Vote::new_committed(3, 1),
                prev_log_id: None,
                entries,
                leader_commit: None,
            }),
        }
    }

    fn blank(i: u64) -> Entry<TypeConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(3, 1), i),
            payload: EntryPayload::Blank,
        }
    }

    fn membership(n: u64) -> openraft::Membership<NodeId, openraft::BasicNode> {
        let nodes: std::collections::BTreeMap<NodeId, openraft::BasicNode> = (1..=n)
            .map(|i| (i, openraft::BasicNode::new(format!("h{i}:1"))))
            .collect();
        openraft::Membership::new(vec![nodes.keys().copied().collect()], nodes)
    }

    fn decode_msg(m: &ClientMsg) -> Result<ClientMsg, FrameError> {
        // Encoded with a large limit: the sender side does not check.
        let f = encode(m, usize::MAX >> 1).expect("encode");
        decode::<ClientMsg>(&f, usize::MAX >> 1).map(|r| r.expect("complete").0)
    }

    /// L1: collections that a few bytes each could expand into large
    /// allocations are bounded while decoding.
    #[test]
    fn decoding_bounds_entries_items_and_memberships() {
        // Entries: at the limit fine, beyond it a decode error.
        let ok = append_with((1..=MAX_APPEND_ENTRIES as u64).map(blank).collect());
        let got = decode_msg(&ok).expect("at the limit");
        assert_eq!(format!("{got:?}"), format!("{ok:?}"));
        let over = append_with((1..=MAX_APPEND_ENTRIES as u64 + 1).map(blank).collect());
        assert!(matches!(decode_msg(&over), Err(FrameError::Decode(_))));

        // An absurd announced length is rejected before any element is
        // decoded: an empty AppendEntries whose entry count (the byte
        // before the final `leader_commit: None`) says 2^40.
        let mut payload = postcard::to_allocvec(&append_with(vec![])).expect("encode");
        let n = payload.len();
        assert_eq!(&payload[n - 2..], &[0, 0]);
        payload.truncate(n - 2);
        payload.extend(postcard::to_allocvec(&(1u64 << 40)).expect("len"));
        payload.push(0);
        assert!(postcard::from_bytes::<ClientMsg>(&payload).is_err());

        let m = Entry {
            log_id: LogId::new(CommittedLeaderId::new(3, 1), 1),
            payload: EntryPayload::Membership(membership(5)),
        };
        let ok = append_with(vec![m]);
        let got = decode_msg(&ok).expect("membership");
        assert_eq!(format!("{got:?}"), format!("{ok:?}"));
        let big = Entry {
            log_id: LogId::new(CommittedLeaderId::new(3, 1), 1),
            payload: EntryPayload::Membership(membership(MAX_MEMBERS as u64 + 1)),
        };
        assert!(matches!(
            decode_msg(&append_with(vec![big])),
            Err(FrameError::Decode(_))
        ));

        let fwd = |n: u64| ClientMsg::Request {
            id: 2,
            body: RpcRequest::Forward(ForwardRequest {
                from: 2,
                items: (1..=n)
                    .map(|i| (2 << 48 | i, 1, EngineInput::Connect(2 << 48 | i)))
                    .collect(),
            }),
        };
        let ok = fwd(MAX_FORWARD_ITEMS as u64);
        assert_eq!(
            format!("{:?}", decode_msg(&ok).expect("at the limit")),
            format!("{ok:?}")
        );
        assert!(matches!(
            decode_msg(&fwd(MAX_FORWARD_ITEMS as u64 + 1)),
            Err(FrameError::Decode(_))
        ));

        // A snapshot chunk whose meta names too many members.
        let snap = |n: u64| ClientMsg::Request {
            id: 3,
            body: RpcRequest::InstallSnapshot(InstallSnapshotRequest {
                vote: Vote::new_committed(3, 1),
                meta: SnapshotMeta {
                    last_log_id: Some(LogId::new(CommittedLeaderId::new(3, 1), 9)),
                    last_membership: openraft::StoredMembership::new(
                        Some(LogId::new(CommittedLeaderId::new(3, 1), 1)),
                        membership(n),
                    ),
                    snapshot_id: "s".into(),
                },
                offset: 0,
                data: vec![1, 2, 3],
                done: true,
            }),
        };
        let ok = snap(3);
        assert_eq!(
            format!("{:?}", decode_msg(&ok).expect("snapshot")),
            format!("{ok:?}")
        );
        assert!(matches!(
            decode_msg(&snap(MAX_MEMBERS as u64 + 1)),
            Err(FrameError::Decode(_))
        ));
    }

    /// The text in a snapshot's meta and in memberships is bounded, so a
    /// peer cannot make a follower store a meta its own store refuses to
    /// read back.
    #[test]
    fn decoding_bounds_meta_text() {
        let snap = |id: usize, addr: usize| ClientMsg::Request {
            id: 3,
            body: RpcRequest::InstallSnapshot(InstallSnapshotRequest {
                vote: Vote::new_committed(3, 1),
                meta: SnapshotMeta {
                    last_log_id: Some(LogId::new(CommittedLeaderId::new(3, 1), 9)),
                    last_membership: openraft::StoredMembership::new(
                        Some(LogId::new(CommittedLeaderId::new(3, 1), 1)),
                        openraft::Membership::new(
                            vec![[1].into()],
                            std::collections::BTreeMap::from([(
                                1,
                                openraft::BasicNode::new("a".repeat(addr)),
                            )]),
                        ),
                    ),
                    snapshot_id: "i".repeat(id),
                },
                offset: 0,
                data: vec![],
                done: false,
            }),
        };
        let ok = snap(MAX_SNAPSHOT_ID_LEN, MAX_NODE_ADDR_LEN);
        assert_eq!(
            format!("{:?}", decode_msg(&ok).expect("at the limits")),
            format!("{ok:?}")
        );
        assert!(matches!(
            decode_msg(&snap(MAX_SNAPSHOT_ID_LEN + 1, 1)),
            Err(FrameError::Decode(_))
        ));
        assert!(matches!(
            decode_msg(&snap(1, MAX_NODE_ADDR_LEN + 1)),
            Err(FrameError::Decode(_))
        ));
        // The same address limit holds for a membership entry.
        let long = Entry {
            log_id: LogId::new(CommittedLeaderId::new(3, 1), 1),
            payload: EntryPayload::Membership(openraft::Membership::new(
                vec![[1].into()],
                std::collections::BTreeMap::from([(
                    1,
                    openraft::BasicNode::new("a".repeat(MAX_NODE_ADDR_LEN + 1)),
                )]),
            )),
        };
        assert!(matches!(
            decode_msg(&append_with(vec![long])),
            Err(FrameError::Decode(_))
        ));
    }

    /// P3-FD: `Op::Batch` items are bounded in log entries and control
    /// requests, and every older `Op` variant keeps its encoding.
    #[test]
    fn decoding_bounds_batches() {
        let batch = |n: u64| {
            Op::Batch(
                (1..=n)
                    .map(|i| (1, EngineInput::Connect(2 << 48 | i)))
                    .collect(),
            )
        };
        let entry = |op: Op| Entry {
            log_id: LogId::new(CommittedLeaderId::new(3, 1), 1),
            payload: EntryPayload::Normal(Request { now: 5, op }),
        };
        let ok = append_with(vec![entry(batch(MAX_BATCH_ITEMS as u64))]);
        assert_eq!(
            format!("{:?}", decode_msg(&ok).expect("at the limit")),
            format!("{ok:?}")
        );
        let over = append_with(vec![entry(batch(MAX_BATCH_ITEMS as u64 + 1))]);
        assert!(matches!(decode_msg(&over), Err(FrameError::Decode(_))));

        let ctl = |op: Op| ClientMsg::Request {
            id: 4,
            body: RpcRequest::Control(ControlRequest { from: 2, op }),
        };
        let ok = ctl(batch(3));
        assert_eq!(
            format!("{:?}", decode_msg(&ok).expect("control")),
            format!("{ok:?}")
        );
        assert!(matches!(
            decode_msg(&ctl(batch(MAX_BATCH_ITEMS as u64 + 1))),
            Err(FrameError::Decode(_))
        ));

        // The older variants round-trip through the mirror, and their
        // encoding is the derived one (old logs and peers stay readable).
        for op in [
            Op::Conn {
                seq: 3,
                input: EngineInput::HalfClose(2 << 48 | 1),
            },
            Op::Tick,
            Op::SetDraining(true),
            Op::DropNode {
                node: 2,
                up_to_local: 9,
            },
            batch(2),
        ] {
            let m = append_with(vec![entry(op.clone())]);
            assert_eq!(
                format!("{:?}", decode_msg(&m).expect("round trip")),
                format!("{m:?}")
            );
            let bytes = postcard::to_allocvec(&op).expect("encode");
            let back: Op = postcard::from_bytes(&bytes).expect("decode");
            assert_eq!(back, op);
        }
        // Variant indexes of the pre-P3-FD variants are unchanged.
        assert_eq!(postcard::to_allocvec(&Op::Tick).expect("encode"), [1]);
        assert_eq!(
            postcard::to_allocvec(&Op::SetDraining(false)).expect("encode"),
            [2, 0]
        );
        assert_eq!(postcard::to_allocvec(&batch(0)).expect("encode"), [4, 0]);
    }

    fn lid(term: u64, index: u64) -> LogId<NodeId> {
        LogId::new(CommittedLeaderId::new(term, 1), index)
    }

    /// A joint membership: voters {1,2,3} → {2,3,4}, learner 5.
    fn sample_status_ex() -> NodeStatusEx {
        let nodes = (1..=5).map(|i| (i, format!("10.0.0.{i}:11400"))).collect();
        NodeStatusEx {
            status: NodeStatus {
                vote: Some(Vote::new_committed(7, 2)),
                last_log_id: Some(lid(7, 40)),
                committed: Some(lid(7, 39)),
                has_state: true,
            },
            raft_running: true,
            rejoining: false,
            term: 7,
            leader: Some(2),
            last_applied: Some(lid(7, 39)),
            highest_member: 5,
            membership: crate::status::MembershipView {
                log_id: Some(lid(7, 38)),
                committed: true,
                configs: vec![[1, 2, 3].into(), [2, 3, 4].into()],
                nodes,
            },
        }
    }

    fn admin_requests() -> Vec<AdminRequest> {
        vec![
            AdminRequest::Membership,
            AdminRequest::AddLearner {
                id: 6,
                addr: "10.0.0.6:11400".into(),
                expect: Some(lid(7, 38)),
            },
            AdminRequest::Promote {
                ids: [4, 6].into(),
                expect: Some(lid(7, 38)),
            },
            AdminRequest::Remove {
                id: 1,
                expect: None,
            },
            AdminRequest::SetAddr {
                id: 3,
                addr: "[::1]:11400".into(),
                expect: Some(lid(7, 38)),
            },
        ]
    }

    fn admin_responses() -> Vec<AdminResponse> {
        vec![
            AdminResponse::Membership(Box::new(sample_status_ex())),
            AdminResponse::Started,
            AdminResponse::Done {
                log_id: Some(lid(7, 41)),
            },
            AdminResponse::NotLeader {
                leader: Some(2),
                addr: Some("10.0.0.2:11400".into()),
            },
            AdminResponse::NotLeader {
                leader: None,
                addr: None,
            },
            AdminResponse::Conflict {
                current: Some(lid(7, 38)),
            },
            AdminResponse::Refused {
                reason: "fewer than 3 voters".into(),
            },
            AdminResponse::Unsupported,
        ]
    }

    /// Every version 4 message, in both directions.
    fn v4_client_msgs() -> Vec<ClientMsg> {
        let mut v = vec![
            ClientMsg::AdminHello(AdminHello {
                version: PROTOCOL_VERSION,
                to: Some(1),
            }),
            ClientMsg::AdminHello(AdminHello {
                version: PROTOCOL_VERSION,
                to: None,
            }),
            ClientMsg::Request {
                id: 14,
                body: RpcRequest::StatusEx,
            },
            ClientMsg::ProbeHello(Hello {
                version: PROTOCOL_VERSION,
                from: 4,
                to: 1,
                max_job_size: 65535,
            }),
        ];
        v.extend(
            admin_requests()
                .into_iter()
                .zip(20..)
                .map(|(body, id)| ClientMsg::Admin { id, body }),
        );
        v
    }

    fn v4_server_msgs() -> Vec<ServerMsg> {
        let mut v = vec![
            ServerMsg::Response {
                id: 14,
                body: RpcResponse::StatusEx(Ok(Box::new(sample_status_ex()))),
            },
            ServerMsg::Response {
                id: 15,
                body: RpcResponse::StatusEx(Ok(Box::default())),
            },
            ServerMsg::Response {
                id: 16,
                body: RpcResponse::StatusEx(Err(WireError::Rejected("no".into()))),
            },
        ];
        v.extend(
            admin_responses()
                .into_iter()
                .zip(20..)
                .map(|(body, id)| ServerMsg::Admin { id, body }),
        );
        v
    }

    fn round_trip<T: Serialize + DeserializeOwned + fmt::Debug>(m: &T) {
        let frame = encode(m, DEFAULT_MAX_FRAME).expect("encode");
        let (got, used): (T, usize) = decode(&frame, DEFAULT_MAX_FRAME)
            .expect("decode")
            .expect("complete");
        assert_eq!(used, frame.len());
        assert_eq!(format!("{got:?}"), format!("{m:?}"));
        assert_eq!(encode(&got, DEFAULT_MAX_FRAME).expect("re"), frame);
    }

    /// P6-T2: the version 4 messages round-trip, and the views' helpers
    /// read a joint configuration correctly.
    #[test]
    fn version_4_messages_round_trip() {
        for m in v4_client_msgs() {
            round_trip(&m);
        }
        for m in v4_server_msgs() {
            round_trip(&m);
        }
        let v = sample_status_ex().membership;
        assert!(v.is_joint());
        assert_eq!(v.voters(), [1, 2, 3, 4].into());
        assert_eq!(v.learners(), [5].into());
        assert!(v.is_member(5) && !v.is_member(6));
        assert!(!admin_requests()[0].is_change());
        assert!(admin_requests()[1..].iter().all(AdminRequest::is_change));
        // A view built from openraft's membership.
        let m = openraft::Membership::new(
            vec![[1, 2].into(), [2, 3].into()],
            (1..=4)
                .map(|i| (i, openraft::BasicNode::new(format!("h{i}:1"))))
                .collect::<std::collections::BTreeMap<_, _>>(),
        );
        let stored = openraft::StoredMembership::new(Some(lid(2, 9)), m);
        let v = crate::status::MembershipView::from_stored(&stored, false);
        assert_eq!(v.log_id, Some(lid(2, 9)));
        assert_eq!(v.voters(), [1, 2, 3].into());
        assert_eq!(v.learners(), [4].into());
        assert_eq!(v.nodes[&4], "h4:1");
    }

    /// P6-T2: the version 3 messages keep their encoding (variant indexes
    /// and the hello's layout); version 4 only appends variants.
    #[test]
    fn version_3_encodings_are_unchanged() {
        let hello = ClientMsg::Hello(Hello {
            version: 3,
            from: 2,
            to: 1,
            max_job_size: 100,
        });
        assert_eq!(
            postcard::to_allocvec(&hello).expect("encode"),
            [0, 3, 2, 1, 100]
        );
        let tag = |m: &ClientMsg| postcard::to_allocvec(m).expect("encode")[0];
        let req = |id, body| ClientMsg::Request { id, body };
        assert_eq!(tag(&req(1, RpcRequest::Status)), 1);
        assert_eq!(
            postcard::to_allocvec(&req(1, RpcRequest::Status)).expect("encode"),
            [1, 1, 5]
        );
        assert_eq!(
            postcard::to_allocvec(&req(1, RpcRequest::StatusEx)).expect("encode"),
            [1, 1, 6]
        );
        assert_eq!(tag(&v4_client_msgs()[0]), 2);
        assert_eq!(tag(&v4_client_msgs()[4]), 3);
        // P6-T3: appended after `Admin`.
        assert_eq!(tag(&v4_client_msgs()[3]), 4);
        let stag = |m: &ServerMsg| postcard::to_allocvec(m).expect("encode")[0];
        assert_eq!(
            stag(&ServerMsg::Hello(ServerHello::Rejected {
                reason: "x".into()
            })),
            0
        );
        let status = ServerMsg::Response {
            id: 1,
            body: RpcResponse::status(NodeStatus::default()),
        };
        assert_eq!(
            postcard::to_allocvec(&status).expect("encode"),
            [1, 1, 5, 0, 0, 0, 0]
        );
        assert_eq!(
            postcard::to_allocvec(&v4_server_msgs()[0]).expect("e")[..3],
            [1, 14, 6]
        );
        assert_eq!(stag(&v4_server_msgs()[3]), 2);
    }

    fn decode_server(m: &ServerMsg) -> Result<ServerMsg, FrameError> {
        let f = encode(m, usize::MAX >> 1).expect("encode");
        decode::<ServerMsg>(&f, usize::MAX >> 1).map(|r| r.expect("complete").0)
    }

    /// P6-T2: memberships in StatusEx answers and admin answers, and the
    /// ids, addresses and reasons of admin messages, are bounded while
    /// decoding (the dialer and the admin tool decode the answers).
    #[test]
    fn version_4_decoding_is_bounded() {
        let with_view = |f: &dyn Fn(&mut crate::status::MembershipView)| {
            let mut s = sample_status_ex();
            f(&mut s.membership);
            [
                ServerMsg::Response {
                    id: 1,
                    body: RpcResponse::StatusEx(Ok(Box::new(s.clone()))),
                },
                ServerMsg::Admin {
                    id: 1,
                    body: AdminResponse::Membership(Box::new(s)),
                },
            ]
        };
        let many_nodes = |n: u64| {
            move |v: &mut crate::status::MembershipView| {
                v.nodes = (1..=n).map(|i| (i, format!("h{i}:1"))).collect();
            }
        };
        let many_voters = |n: u64| {
            move |v: &mut crate::status::MembershipView| v.configs = vec![(1..=n).collect()]
        };
        let joints =
            |n: usize| move |v: &mut crate::status::MembershipView| v.configs = vec![[1].into(); n];
        let addr = |n: usize| {
            move |v: &mut crate::status::MembershipView| {
                v.nodes = [(1, "a".repeat(n))].into();
            }
        };
        for (ok, over) in [
            (
                with_view(&many_nodes(MAX_MEMBERS as u64)),
                with_view(&many_nodes(MAX_MEMBERS as u64 + 1)),
            ),
            (
                with_view(&many_voters(MAX_MEMBERS as u64)),
                with_view(&many_voters(MAX_MEMBERS as u64 + 1)),
            ),
            (
                with_view(&joints(MAX_JOINT_CONFIGS)),
                with_view(&joints(MAX_JOINT_CONFIGS + 1)),
            ),
            (
                with_view(&addr(MAX_NODE_ADDR_LEN)),
                with_view(&addr(MAX_NODE_ADDR_LEN + 1)),
            ),
        ] {
            for m in &ok {
                let got = decode_server(m).expect("at the limit");
                assert_eq!(format!("{got:?}"), format!("{m:?}"));
            }
            for m in &over {
                assert!(matches!(decode_server(m), Err(FrameError::Decode(_))));
            }
        }
        // The largest legal view fits an answer frame comfortably.
        let biggest = with_view(&|v| {
            v.configs = vec![(1..=MAX_MEMBERS as u64).collect(); 2];
            v.nodes = (1..=MAX_MEMBERS as u64)
                .map(|i| (i, "a".repeat(MAX_NODE_ADDR_LEN)))
                .collect();
        });
        let len = encode(&biggest[1], DEFAULT_MAX_FRAME)
            .expect("encode")
            .len();
        assert!(len < 300 * 1024, "{len}");

        let admin_resp = |body| ServerMsg::Admin { id: 1, body };
        for (ok, over) in [
            (
                AdminResponse::NotLeader {
                    leader: Some(2),
                    addr: Some("a".repeat(MAX_NODE_ADDR_LEN)),
                },
                AdminResponse::NotLeader {
                    leader: Some(2),
                    addr: Some("a".repeat(MAX_NODE_ADDR_LEN + 1)),
                },
            ),
            (
                AdminResponse::Refused {
                    reason: "r".repeat(MAX_ADMIN_REASON_LEN),
                },
                AdminResponse::Refused {
                    reason: "r".repeat(MAX_ADMIN_REASON_LEN + 1),
                },
            ),
        ] {
            assert!(decode_server(&admin_resp(ok)).is_ok());
            assert!(matches!(
                decode_server(&admin_resp(over)),
                Err(FrameError::Decode(_))
            ));
        }

        let admin = |body| ClientMsg::Admin { id: 1, body };
        let long = |n: usize| "a".repeat(n);
        for (ok, over) in [
            (
                AdminRequest::AddLearner {
                    id: 9,
                    addr: long(MAX_NODE_ADDR_LEN),
                    expect: None,
                },
                AdminRequest::AddLearner {
                    id: 9,
                    addr: long(MAX_NODE_ADDR_LEN + 1),
                    expect: None,
                },
            ),
            (
                AdminRequest::SetAddr {
                    id: 9,
                    addr: long(MAX_NODE_ADDR_LEN),
                    expect: None,
                },
                AdminRequest::SetAddr {
                    id: 9,
                    addr: long(MAX_NODE_ADDR_LEN + 1),
                    expect: None,
                },
            ),
            (
                AdminRequest::Promote {
                    ids: (1..=MAX_MEMBERS as u64).collect(),
                    expect: None,
                },
                AdminRequest::Promote {
                    ids: (1..=MAX_MEMBERS as u64 + 1).collect(),
                    expect: None,
                },
            ),
        ] {
            let m = admin(ok);
            assert_eq!(
                format!("{:?}", decode_msg(&m).expect("at the limit")),
                format!("{m:?}")
            );
            // The listener reads admin requests with the admin limit.
            let f = encode(&m, ADMIN_MAX_REQUEST_FRAME).expect("fits the admin limit");
            assert!(decode::<ClientMsg>(&f, ADMIN_MAX_REQUEST_FRAME).is_ok());
            assert!(matches!(
                decode_msg(&admin(over)),
                Err(FrameError::Decode(_))
            ));
        }

        // An absurd announced id count is refused before allocating: a
        // Promote whose set length says 2^40.
        let mut payload = postcard::to_allocvec(&admin(AdminRequest::Promote {
            ids: Default::default(),
            expect: None,
        }))
        .expect("encode");
        let n = payload.len();
        assert_eq!(&payload[n - 2..], &[0, 0]);
        payload.truncate(n - 2);
        payload.extend(postcard::to_allocvec(&(1u64 << 40)).expect("len"));
        payload.push(0);
        assert!(postcard::from_bytes::<ClientMsg>(&payload).is_err());
    }

    /// P6-T2: a version 4 frame cut anywhere is incomplete, and a frame
    /// whose header announces fewer bytes than the message needs is a
    /// decode error, never a panic or a partial message.
    #[test]
    fn version_4_truncated_frames() {
        fn check<T: Serialize + DeserializeOwned>(m: &T) {
            let frame = encode(m, DEFAULT_MAX_FRAME).expect("encode");
            for cut in 0..frame.len() {
                assert!(
                    decode::<T>(&frame[..cut], DEFAULT_MAX_FRAME)
                        .expect("no error")
                        .is_none()
                );
            }
            let payload = &frame[HEADER_LEN..];
            for short in 0..payload.len() {
                let mut f = (short as u32).to_be_bytes().to_vec();
                f.extend_from_slice(&payload[..short]);
                assert!(
                    matches!(
                        decode::<T>(&f, DEFAULT_MAX_FRAME),
                        Err(FrameError::Decode(_))
                    ),
                    "cut at {short} of {}",
                    payload.len()
                );
            }
        }
        for m in v4_client_msgs() {
            check(&m);
        }
        for m in v4_server_msgs() {
            check(&m);
        }
    }

    #[test]
    fn peer_text_is_escaped_and_truncated() {
        assert_eq!(sanitize("plain reason"), "plain reason");
        assert_eq!(sanitize("a\nb\u{1b}[31m"), "a\\nb\\u{1b}[31m");
        let long = "x".repeat(10_000);
        let s = sanitize(&long);
        assert_eq!(s.len(), MAX_PEER_TEXT + 3);
        assert!(s.ends_with("..."));
    }

    #[test]
    fn wire_errors_convert_back() {
        let e = WireError::from_raft(&RaftError::Fatal(Fatal::Stopped));
        assert_eq!(e, WireError::Fatal(WireFatal::Stopped));
        let WireError::Fatal(f) = e else {
            panic!("fatal expected")
        };
        assert!(matches!(f.into_fatal(), Fatal::Stopped));
        let s = WireFatal::Storage("disk".into()).into_fatal();
        assert!(s.to_string().contains("disk"), "{s}");
    }
}
