//! Serves the cluster port's forwards and control requests
//! (`bstk_raft::forward::ForwardHandler`).
//!
//! - A forward (another node's connection inputs, in order): on the leader,
//!   each item is proposed with `client_write_ff`, in order, and the
//!   answer is `Accepted` without waiting for the commit (requests of one
//!   peer connection are served one at a time, and Raft RPCs on it wait
//!   behind them). Elsewhere the answer is `NotLeader` with the leader this
//!   node knows of.
//! - A forward without items is a *ping* (an owner checking that the
//!   leader hears it, see [`super::actor`]): `Accepted` on the leader,
//!   elsewhere `NotLeader` with the leader this node knows. (Nodes deciding
//!   how to start ask with status probes instead, which the listener
//!   answers from the log store.)
//! - A control request (`SetDraining`, or `DropNode` of the sender, checked
//!   by the listener): on the leader it is proposed and the answer waits,
//!   at most one second, for it to be applied, so that the requester knows
//!   its index. These are rare (SIGUSR1, a node's startup and rejoin).
//!
//! Every forward, ping and control request records that its sender was
//! heard from (the leader's node liveness, [`super::duties`]).

use std::sync::Arc;

use bstk_raft::forward::{ControlRequest, ControlResponse, ForwardHandler};
use bstk_raft::{ForwardRequest, ForwardResponse, Op};

use super::{ControlOutcome, Core};

pub struct Handler {
    core: Arc<Core>,
}

impl Handler {
    pub fn new(core: Arc<Core>) -> Handler {
        Handler { core }
    }

    fn not_leader(&self) -> Option<u64> {
        self.core.leader().filter(|&l| l != self.core.id)
    }
}

impl ForwardHandler for Handler {
    async fn forward(&self, req: ForwardRequest) -> ForwardResponse {
        self.core.heard_from(req.from);
        if !self.core.is_leader() {
            return ForwardResponse::NotLeader {
                leader: self.not_leader(),
            };
        }
        for (_conn, seq, input) in req.items {
            if self.core.propose(Op::Conn { seq, input }).await.is_err() {
                return ForwardResponse::NotLeader { leader: None };
            }
        }
        ForwardResponse::Accepted
    }

    async fn control(&self, req: ControlRequest) -> ControlResponse {
        self.core.heard_from(req.from);
        if !self.core.is_leader() {
            return ControlResponse::NotLeader {
                leader: self.not_leader(),
            };
        }
        match self.core.propose_and_wait(req.op).await {
            ControlOutcome::Applied(i) => ControlResponse::Accepted { index: Some(i) },
            ControlOutcome::Unknown => ControlResponse::Accepted { index: None },
            ControlOutcome::NotProposed => ControlResponse::NotLeader { leader: None },
        }
    }
}
