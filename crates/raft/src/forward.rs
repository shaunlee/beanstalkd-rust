//! Input forwarding (owner → leader) interfaces shared by the TCP
//! transport ([`crate::client`], [`crate::listener`]) and the simulated
//! network (`crate::sim`).

use std::fmt;
use std::future::Future;

use serde::{Deserialize, Serialize};

use crate::{ForwardRequest, ForwardResponse, NodeId, Op};

/// A cluster-wide operation a node asks the leader to propose (wire
/// protocol version 2): `Op::SetDraining(_)` (SIGUSR1 on any node), or
/// `Op::DropNode { node: from, .. }` (a restarted node closing out the
/// connections of its previous process). The listener accepts only these
/// two, and `DropNode` only for the sender's own id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlRequest {
    pub from: NodeId,
    pub op: Op,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlResponse {
    /// Proposed. `index` is the proposal's log index if the leader applied
    /// it within its (short) bound, so the requester can wait until it has
    /// applied that index itself; `None` means the outcome is unknown (it
    /// may still commit later).
    Accepted { index: Option<u64> },
    /// Not the leader; resend to `leader` if known.
    NotLeader { leader: Option<NodeId> },
}

/// Serves [`ForwardRequest`]s arriving on the cluster port (implemented by
/// the server in P3-T4).
///
/// The listener calls it only after checking that `req.from` is the
/// authenticated peer and that every item's connection is owned by that
/// peer (`owner_of(conn) == req.from`). Requests of one peer connection are
/// served one at a time, in arrival order, and Raft RPCs on the same
/// connection wait behind it: the implementation must return once the
/// inputs are proposed (for example with `client_write_ff`), never wait for
/// their commit.
pub trait ForwardHandler: Send + Sync + 'static {
    fn forward(&self, req: ForwardRequest) -> impl Future<Output = ForwardResponse> + Send;

    /// Serves a [`ControlRequest`] (called only after the listener checked
    /// it). Control requests are rare (startup, SIGUSR1), so unlike
    /// `forward` the implementation may wait, briefly and with a bound well
    /// below the requester's timeout, for the proposal to be applied. The
    /// default refuses (a handler that never leads).
    fn control(&self, req: ControlRequest) -> impl Future<Output = ControlResponse> + Send {
        let _ = req;
        async { ControlResponse::NotLeader { leader: None } }
    }
}

/// The client side of forwarding, implemented by the TCP network
/// ([`crate::client::Network`]) and the simulated one.
pub trait ForwardTransport: Clone + Send + Sync + 'static {
    /// Sends `req` to `target` and waits for its answer. There is no
    /// automatic resend: the caller decides (P3-T4) what to resend and
    /// where after an error or `NotLeader`.
    fn forward(
        &self,
        target: NodeId,
        req: ForwardRequest,
    ) -> impl Future<Output = Result<ForwardResponse, ForwardError>> + Send;
}

/// Why a forward got no answer. After any of these the request may or may
/// not have been proposed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ForwardError {
    /// No connection to the target (dial failed, backing off, or unknown
    /// target).
    Unreachable(String),
    /// No answer within the forward timeout.
    Timeout,
    /// The connection failed while the request was in flight.
    Network(String),
    /// The target refused the request (for example `from` is not the
    /// authenticated peer).
    Rejected(String),
}

impl fmt::Display for ForwardError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ForwardError::Unreachable(m) => write!(f, "unreachable: {m}"),
            ForwardError::Timeout => f.write_str("timed out"),
            ForwardError::Network(m) => write!(f, "network error: {m}"),
            ForwardError::Rejected(m) => write!(f, "rejected: {m}"),
        }
    }
}

impl std::error::Error for ForwardError {}

/// Checks the listener applies to a forward from authenticated `peer`.
pub(crate) fn check_forward(peer: NodeId, req: &ForwardRequest) -> Result<(), String> {
    if req.from != peer {
        return Err(format!(
            "forward from node {} on the connection of node {peer}",
            req.from
        ));
    }
    if let Some((conn, _, _)) = req
        .items
        .iter()
        .find(|(conn, _, _)| crate::owner_of(*conn) != peer)
    {
        return Err(format!("connection {conn} is not owned by node {peer}"));
    }
    if let Some((conn, _, _)) = req
        .items
        .iter()
        .find(|(conn, _, input)| input.conn().is_some_and(|c| c != *conn))
    {
        return Err(format!(
            "an input of connection {conn} names another connection"
        ));
    }
    Ok(())
}

/// Checks the listener applies to a control request from authenticated
/// `peer`.
pub(crate) fn check_control(peer: NodeId, req: &ControlRequest) -> Result<(), String> {
    if req.from != peer {
        return Err(format!(
            "control request from node {} on the connection of node {peer}",
            req.from
        ));
    }
    match req.op {
        Op::SetDraining(_) => Ok(()),
        Op::DropNode { node, .. } if node == peer => Ok(()),
        Op::DropNode { node, .. } => Err(format!("node {peer} may not drop node {node}")),
        Op::Conn { .. } | Op::Tick => {
            Err(format!("operation {:?} is not a control operation", req.op))
        }
    }
}
