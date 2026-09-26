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

// Network (P3-T3).
pub mod client;
pub mod forward;
pub mod listener;
#[cfg(test)]
mod net_tests;
#[cfg(any(test, feature = "sim"))]
pub mod sim;
#[cfg(test)]
mod sim_tests;
pub mod status;
#[cfg(test)]
mod test_store;
pub mod tls;
pub mod wire;

// Storage: log store, state machine, snapshots (P3-T2).
pub mod snapshot_buf;
pub mod storage;

pub use snapshot_buf::SnapshotBuf;

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
    /// The node is gone: disconnect every connection owned by `node` whose
    /// local number is at most `up_to_local`, in ascending `ConnId` order.
    /// The bound is the `highest_local(node)` the proposer observed, so a
    /// `DropNode` that commits late (for example a leader's, proposed for a
    /// node that has meanwhile restarted) never touches the connections the
    /// node accepted afterwards: a restarted node numbers its new
    /// connections above every local number the state has seen for it.
    DropNode { node: NodeId, up_to_local: u64 },
    /// Several connection inputs in one log entry (P3-FD: the leader
    /// proposes everything it has queued or received in one entry, so one
    /// log write and one `fdatasync` cover many inputs). Items are
    /// `(seq, input)` exactly as in [`Op::Conn`]; they may belong to
    /// connections of several owners, in the proposer's order (each
    /// owner's items in that owner's order). The state machine applies
    /// them one by one, in order, each with the dedup rules of `Op::Conn`
    /// and at the entry's `now` (every applied input is followed by the
    /// engine's timer pass at that `now`, as for `Op::Conn`), so a batch
    /// applies exactly like the same items proposed as consecutive
    /// `Op::Conn` entries with the same `now`. An item that is not a
    /// connection input (`Tick`, `SetDraining`) is ignored. The applied
    /// result's `duplicate` is true when every item was ignored.
    /// Decoding from the cluster port is bounded by
    /// [`wire::MAX_BATCH_ITEMS`]; the server proposes at most
    /// [`MAX_PROPOSAL_ITEMS`] items and, unless the batch is a single
    /// item, at most [`MAX_PROPOSAL_BYTES`] of job bodies per entry.
    /// `Op::Conn` stays valid (logs written before P3-FD replay as they
    /// are). Declared last so that the encoding of the older variants is
    /// unchanged.
    Batch(Vec<(u64, EngineInput)>),
}

/// Most items the server puts in one [`Op::Batch`].
pub const MAX_PROPOSAL_ITEMS: usize = 1024;

/// Most bytes of job bodies (plus a fixed overhead per item, see
/// [`proposal_item_size`]) the server puts in one [`Op::Batch`], unless
/// the batch is a single item (a job body may be up to the frame size
/// alone).
pub const MAX_PROPOSAL_BYTES: usize = 1 << 20;

/// The size an input counts for in [`MAX_PROPOSAL_BYTES`]: 64 bytes plus
/// its job body, if any.
pub fn proposal_item_size(input: &EngineInput) -> usize {
    64 + match input {
        EngineInput::Command {
            cmd: bstk_proto::Command::Put { body, .. },
            ..
        } => body.len(),
        _ => 0,
    }
}

/// Splits `items` into batches of at most [`MAX_PROPOSAL_ITEMS`] items
/// and [`MAX_PROPOSAL_BYTES`] (a single larger item forms a batch of its
/// own), keeping their order.
pub fn split_batches(items: Vec<(u64, EngineInput)>) -> Vec<Vec<(u64, EngineInput)>> {
    let mut out = Vec::new();
    let mut cur: Vec<(u64, EngineInput)> = Vec::new();
    let mut bytes = 0;
    for (seq, input) in items {
        let size = proposal_item_size(&input);
        if !cur.is_empty() && (cur.len() >= MAX_PROPOSAL_ITEMS || bytes + size > MAX_PROPOSAL_BYTES)
        {
            out.push(std::mem::take(&mut cur));
            bytes = 0;
        }
        bytes += size;
        cur.push((seq, input));
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// The apply result returned to the proposer (openraft `R`). Replies to
/// clients never travel here; owners compute them from their own apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Applied {
    /// The entry was a duplicate `Op::Conn` and was ignored (for an
    /// `Op::Batch`: every item was ignored).
    pub duplicate: bool,
}

openraft::declare_raft_types!(
    /// openraft type configuration for beanstalkd-rs.
    pub TypeConfig:
        D = Request,
        R = Applied,
        NodeId = NodeId,
        Node = openraft::BasicNode,
        // Bounded, gap-free receive buffer (see `snapshot_buf`).
        SnapshotData = SnapshotBuf,
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn put(len: usize) -> EngineInput {
        EngineInput::Command {
            conn: 1,
            cmd: bstk_proto::Command::Put {
                pri: 0,
                delay: 0,
                ttr: 1,
                body: vec![b'x'; len].into(),
            },
        }
    }

    #[test]
    fn split_batches_respects_the_bounds_in_order() {
        let items: Vec<(u64, EngineInput)> =
            (1..=2500).map(|i| (i, EngineInput::Connect(i))).collect();
        let sizes: Vec<usize> = split_batches(items).iter().map(Vec::len).collect();
        assert_eq!(sizes, [1024, 1024, 452]);

        // 300 KiB bodies: three per MiB; a 3 MiB body goes alone.
        let mut items: Vec<(u64, EngineInput)> = (1..=4).map(|i| (i, put(300 << 10))).collect();
        items.push((5, put(3 << 20)));
        items.push((6, put(10)));
        let seqs: Vec<Vec<u64>> = split_batches(items)
            .iter()
            .map(|b| b.iter().map(|(s, _)| *s).collect())
            .collect();
        assert_eq!(seqs, [vec![1, 2, 3], vec![4], vec![5], vec![6]]);
        assert!(split_batches(Vec::new()).is_empty());
    }
}
