//! Raft replication of the engine (P3), on openraft 0.9.
//!
//! INTERFACE CONTRACT (owned by the lead): the public types in this file
//! must not change without lead approval. See docs/PLAN.md §6 and
//! docs/DESIGN.md §8.
//!
//! Model: every engine input is a log entry ([`Request`]). Every node
//! applies committed entries to its own `Engine` in log order and computes
//! every reply; each node delivers only the replies for connections it owns
//! ([`owner_of`]). Nothing is acknowledged to a client before its input is
//! committed on a majority.

use std::io::Cursor;

use bstk_engine::{ConnId, EngineInput, Nanos};
use serde::{Deserialize, Serialize};

/// Cluster node id: 1..=[`MAX_NODE_ID`].
pub type NodeId = u64;

/// Node ids must fit in the high 16 bits of a [`ConnId`].
pub const MAX_NODE_ID: NodeId = u16::MAX as NodeId;

/// Bits of a [`ConnId`] holding the owner-local sequence number.
pub const CONN_SEQ_BITS: u32 = 48;

/// The cluster-unique id of connection number `local` accepted by `node`.
/// Panics (debug) if `node` is out of range or `local` does not fit.
pub fn conn_id(node: NodeId, local: u64) -> ConnId {
    debug_assert!((1..=MAX_NODE_ID).contains(&node));
    debug_assert!(local < (1 << CONN_SEQ_BITS));
    (node << CONN_SEQ_BITS) | local
}

/// The node that holds the socket of `conn`.
pub fn owner_of(conn: ConnId) -> NodeId {
    conn >> CONN_SEQ_BITS
}

/// A log entry payload (openraft `D`). `now` is stamped by the leader when
/// it proposes the entry: `max(leader's wall-anchored clock, last applied
/// now)`, so engine time never goes backwards across leader changes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Request {
    pub now: Nanos,
    pub op: Op,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Op {
    /// A connection's input, forwarded by its owner. `seq` increases by one
    /// per input of that connection, starting at 1 with `Connect`. The state
    /// machine applies an input only if `seq` is the connection's next one,
    /// and a `Connect` only if the connection's local number is above every
    /// local number of the same owner connected so far (owners forward in
    /// order, and connection ids are never reused), so resends after a
    /// leader change, and late duplicates of a closed connection's inputs,
    /// are ignored.
    Conn { seq: u64, input: EngineInput },
    /// A timer is due (proposed by the leader at `next_deadline()`), or a
    /// no-op after a leader change. Applies `EngineInput::Tick`.
    Tick,
    /// Cluster-wide drain mode (SIGUSR1 on any node).
    SetDraining(bool),
    /// The node is gone: disconnect every connection it owns, in ascending
    /// `ConnId` order.
    DropNode(NodeId),
}

/// The apply result returned to the proposer (openraft `R`). Replies to
/// clients never travel here; owners compute them from their own apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Applied {
    /// The entry was a duplicate `Op::Conn` and was ignored.
    pub duplicate: bool,
}

openraft::declare_raft_types!(
    /// openraft type configuration for beanstalkd-rs.
    pub TypeConfig:
        D = Request,
        R = Applied,
        NodeId = NodeId,
        Node = openraft::BasicNode,
        SnapshotData = Cursor<Vec<u8>>,
);

/// Owner → leader: inputs of the owner's connections, in order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForwardRequest {
    pub from: NodeId,
    /// `(conn, seq, input)`; for each connection, in increasing `seq`.
    pub items: Vec<(ConnId, u64, EngineInput)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ForwardResponse {
    /// Proposed (not necessarily committed yet).
    Accepted,
    /// Not the leader; resend to `leader` if known.
    NotLeader { leader: Option<NodeId> },
}
