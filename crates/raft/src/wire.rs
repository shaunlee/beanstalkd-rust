//! Wire protocol of the cluster port (docs/DESIGN.md §8).
//!
//! A connection carries frames in both directions. A frame is a `u32`
//! big-endian payload length followed by that many bytes of a postcard
//! message. Frames longer than the configured maximum are rejected and the
//! connection is closed; nothing larger than the maximum is ever allocated.
//!
//! The dialer sends [`ClientMsg::Hello`] first; the listener answers with
//! [`ServerMsg::Hello`] (accepted or rejected) and then serves
//! [`ClientMsg::Request`]s, answering each with a [`ServerMsg::Response`]
//! carrying the same request id. Remote failures are values
//! ([`WireError`]), never a dropped connection.
//!
//! openraft's own error types are not sent as is: `StorageError` embeds a
//! recursive `AnyError` chain, and a deeply nested value could exhaust the
//! receiver's stack while decoding. [`WireError`] is flat.
//!
//! Decoding is bounded: a frame is at most `max_frame` bytes, and the
//! collections in requests that a few bytes each could expand into large
//! allocations are limited while decoding (before they are built):
//! AppendEntries entries ([`MAX_APPEND_ENTRIES`]), forward items
//! ([`MAX_FORWARD_ITEMS`]), and the node sets of memberships
//! ([`MAX_MEMBERS`], [`MAX_JOINT_CONFIGS`]). A request beyond a limit is a
//! decode error, which closes the connection. A status probe and its
//! answer ([`RpcRequest::Status`], [`RpcResponse::Status`]) have a fixed
//! size (no collections), so they need no bound of their own.

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
use crate::status::NodeStatus;
use crate::{ForwardRequest, ForwardResponse, NodeId, TypeConfig};

/// Version of this wire protocol, carried in the hellos.
///
/// - 1: P3-T3 (Raft RPCs and input forwarding).
/// - 2: P3-T4: the hellos carry `max_job_size` (peers with a different
///   `-z` are rejected), and control requests ([`RpcRequest::Control`]).
/// - 3: P3-FC: status probes ([`RpcRequest::Status`]), answered from the
///   log store even before Raft runs (safe rejoin, `--cluster-init`).
pub const PROTOCOL_VERSION: u32 = 3;

/// Bytes of the length prefix.
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

/// Most node ids in one membership config, and nodes in one membership
/// (clusters have 1, 3 or 5 nodes).
pub const MAX_MEMBERS: usize = 256;

/// Most configs in one (joint) membership; openraft uses at most two.
pub const MAX_JOINT_CONFIGS: usize = 4;

/// First frame on a connection, from the dialer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hello {
    pub version: u32,
    /// The dialer's node id. With TLS it must match the client certificate.
    pub from: NodeId,
    /// The node the dialer believes it is connected to.
    pub to: NodeId,
    /// The dialer's `-z`. Every node must use the same value (the engine
    /// replies `JOB_TOO_BIG` by it), so the listener rejects a mismatch.
    pub max_job_size: u32,
}

/// The listener's answer to [`Hello`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServerHello {
    Accepted {
        version: u32,
        node_id: NodeId,
        /// The listener's `-z` (equal to the dialer's, or the hello would
        /// have been rejected); the dialer checks it too.
        max_job_size: u32,
    },
    /// The connection is closed right after this frame.
    Rejected { reason: String },
}

/// Dialer → listener.
#[derive(Debug, Serialize, Deserialize)]
pub enum ClientMsg {
    Hello(Hello),
    Request { id: u64, body: RpcRequest },
}

/// Listener → dialer.
#[derive(Debug, Serialize, Deserialize)]
pub enum ServerMsg {
    Hello(ServerHello),
    Response { id: u64, body: RpcResponse },
}

/// A request. The same encoding as the derived one; decoding applies the
/// limits of the module docs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RpcRequest {
    AppendEntries(#[serde(deserialize_with = "bounded::append")] AppendEntriesRequest<TypeConfig>),
    Vote(VoteRequest<NodeId>),
    /// One chunk of openraft 0.9's chunked snapshot transfer.
    InstallSnapshot(
        #[serde(deserialize_with = "bounded::install")] InstallSnapshotRequest<TypeConfig>,
    ),
    Forward(#[serde(deserialize_with = "bounded::forward")] ForwardRequest),
    /// A cluster-wide operation requested by a non-leader (version 2).
    Control(ControlRequest),
    /// The peer's durable Raft state (version 3), answered from its log
    /// store whether or not its Raft is running (see [`crate::status`]).
    Status,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcResponse {
    AppendEntries(Result<AppendEntriesResponse<NodeId>, WireError>),
    Vote(Result<VoteResponse<NodeId>, WireError>),
    InstallSnapshot(Result<InstallSnapshotResponse<NodeId>, WireError>),
    Forward(Result<ForwardResponse, WireError>),
    Control(Result<ControlResponse, WireError>),
    /// The answer to [`RpcRequest::Status`] (see
    /// [`crate::status::NodeStatus`] for the fields).
    Status {
        vote: Option<Vote<NodeId>>,
        last_log_id: Option<LogId<NodeId>>,
        committed: Option<LogId<NodeId>>,
        has_state: bool,
    },
}

impl RpcResponse {
    /// The status answer for `s`.
    pub fn status(s: NodeStatus) -> RpcResponse {
        RpcResponse::Status {
            vote: s.vote,
            last_log_id: s.last_log_id,
            committed: s.committed,
            has_state: s.has_state,
        }
    }

    /// The status carried by a [`RpcResponse::Status`].
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

/// A remote failure, transported as a value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WireError {
    /// The remote Raft node failed (`RaftError::Fatal`).
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
    /// A storage error, reduced to its message.
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
    /// From the error of `Raft::append_entries` / `Raft::vote`.
    pub fn from_raft(e: &RaftError<NodeId>) -> Self {
        match e {
            RaftError::APIError(never) => match *never {},
            RaftError::Fatal(f) => WireError::Fatal(WireFatal::from_fatal(f)),
        }
    }

    /// From the error of `Raft::install_snapshot`.
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

    use bstk_engine::{ConnId, EngineInput};
    use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest};
    use openraft::{
        BasicNode, Entry, EntryPayload, LogId, Membership, SnapshotMeta, StoredMembership, Vote,
    };
    use serde::Deserialize;
    use serde::de::{self, Deserializer, MapAccess, SeqAccess, Visitor};

    use super::{MAX_APPEND_ENTRIES, MAX_FORWARD_ITEMS, MAX_JOINT_CONFIGS, MAX_MEMBERS};
    use crate::{ForwardRequest, NodeId, Request, TypeConfig};

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

    /// A map of at most `max` entries.
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

    fn entries<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<EntryWire>, D::Error> {
        seq(d, MAX_APPEND_ENTRIES)
    }

    fn items<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<(ConnId, u64, EngineInput)>, D::Error> {
        seq(d, MAX_FORWARD_ITEMS)
    }

    /// A membership config (a set of node ids).
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
        d.deserialize_map(MapVisitor {
            max: MAX_MEMBERS,
            _kv: PhantomData,
        })
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
        Normal(Request),
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
                    PayloadWire::Normal(r) => EntryPayload::Normal(r),
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

/// Why a frame could not be read or written.
#[derive(Debug)]
pub enum FrameError {
    Io(io::Error),
    /// The frame's payload exceeds the maximum.
    TooLarge {
        len: usize,
        max: usize,
    },
    /// The stream ended inside a frame.
    Truncated,
    /// The payload is not a valid message.
    Decode(postcard::Error),
    /// The message could not be serialized.
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

/// Serializes `msg` into a complete frame (header included).
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

/// Writes one already-encoded frame (see [`encode`]) and flushes.
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
        // The status fields survive the round trip.
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
        // Two frames then clean EOF.
        let mut two = frame.clone();
        two.extend(&frame);
        let mut r = two.as_slice();
        for _ in 0..2 {
            let m: Option<ClientMsg> = read_frame(&mut r, DEFAULT_MAX_FRAME).await.expect("read");
            assert_eq!(format!("{m:?}"), format!("{:?}", Some(sample_append())));
        }
        let m: Option<ClientMsg> = read_frame(&mut r, DEFAULT_MAX_FRAME).await.expect("eof");
        assert!(m.is_none());

        // EOF inside the header and inside the payload.
        for cut in [2, HEADER_LEN + 1, frame.len() - 1] {
            let mut r = &frame[..cut];
            let e = read_frame::<_, ClientMsg>(&mut r, DEFAULT_MAX_FRAME).await;
            assert!(matches!(e, Err(FrameError::Truncated)), "cut {cut}");
        }

        // Oversize announced length: rejected without reading the payload.
        let mut r = &u32::MAX.to_be_bytes()[..];
        let e = read_frame::<_, ClientMsg>(&mut r, DEFAULT_MAX_FRAME).await;
        assert!(matches!(e, Err(FrameError::TooLarge { .. })));

        // Garbage payload.
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

        // Membership entries (the leader's normal case) round-trip.
        let m = Entry {
            log_id: LogId::new(CommittedLeaderId::new(3, 1), 1),
            payload: EntryPayload::Membership(membership(5)),
        };
        let ok = append_with(vec![m]);
        let got = decode_msg(&ok).expect("membership");
        assert_eq!(format!("{got:?}"), format!("{ok:?}"));
        // Too many nodes.
        let big = Entry {
            log_id: LogId::new(CommittedLeaderId::new(3, 1), 1),
            payload: EntryPayload::Membership(membership(MAX_MEMBERS as u64 + 1)),
        };
        assert!(matches!(
            decode_msg(&append_with(vec![big])),
            Err(FrameError::Decode(_))
        ));

        // Forward items.
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
