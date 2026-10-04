//! Serves the cluster port's forwards and control requests
//! (`bstk_raft::forward::ForwardHandler`).
//!
//! - A forward (another node's connection inputs, in order): on the leader the
//!   items go, in order, to the [`super::proposer`] and the answer is
//!   `Accepted` without waiting for the proposal or its commit (requests of
//!   one peer connection are served one at a time and Raft RPCs on it wait
//!   behind them). Elsewhere: `NotLeader` with the leader this node knows of.
//!   A forward without items is a *ping* (see [`super::actor`]), answered the
//!   same way.
//! - A control request (`SetDraining`, or `DropNode` of the sender, checked by
//!   the listener): on the leader it is proposed and the answer waits, at most
//!   one second, for it to be applied, so the requester knows its index. These
//!   are rare (SIGUSR1, startup, rejoin).
//!
//! Every forward, ping and control request records that its sender was heard
//! from (node liveness, [`super::duties`]).
//!
//! Forwards and control requests from a node outside the effective
//! membership are refused (`NotLeader` without a leader): the listener closes
//! a node's connection when it leaves the membership, but a request already
//! being served, or one racing the allowlist update, could otherwise still
//! reach the proposer (docs/DESIGN.md §8, "Membership-driven networking").

use std::sync::Arc;

use bstk_raft::forward::{ControlRequest, ControlResponse, ForwardHandler};
use bstk_raft::{ForwardRequest, ForwardResponse};

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

    fn refuse_non_member(&self, from: u64) -> bool {
        if self.core.is_member(from) {
            return false;
        }
        tracing::debug!(from, "request from a node outside the membership refused");
        true
    }
}

impl ForwardHandler for Handler {
    async fn forward(&self, req: ForwardRequest) -> ForwardResponse {
        if self.refuse_non_member(req.from) {
            return ForwardResponse::NotLeader { leader: None };
        }
        self.core.heard_from(req.from);
        if !self.core.is_leader() {
            return ForwardResponse::NotLeader {
                leader: self.not_leader(),
            };
        }
        let items = req
            .items
            .into_iter()
            .map(|(_conn, seq, input)| (seq, input))
            .collect();
        if self.core.submit(items).is_err() {
            return ForwardResponse::NotLeader { leader: None };
        }
        ForwardResponse::Accepted
    }

    async fn control(&self, req: ControlRequest) -> ControlResponse {
        if self.refuse_non_member(req.from) {
            return ControlResponse::NotLeader { leader: None };
        }
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
