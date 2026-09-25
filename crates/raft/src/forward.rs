//! Input forwarding (owner → leader) interfaces shared by the TCP
//! transport ([`crate::client`], [`crate::listener`]) and the simulated
//! network (`crate::sim`).

use std::fmt;
use std::future::Future;

use crate::{ForwardRequest, ForwardResponse, NodeId};

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
