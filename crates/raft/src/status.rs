//! Node status probes (wire protocol version 3) and the startup decisions
//! built on them: safe rejoin and `--cluster-init` (docs/DESIGN.md §8).
//!
//! A node answers [`crate::wire::RpcRequest::Status`] from its log store
//! ([`StatusSource`]), whether or not its Raft is running, so a node that
//! has not started Raft yet (a rejoining node probing its peers, or an
//! initial node deciding whether to bootstrap) can be asked too.
//!
//! # Vote order
//!
//! Every comparison here uses openraft's own `PartialOrd` on `Vote`. In the
//! build this workspace uses (without openraft's `single-term-leader`
//! feature) a vote is ordered by its leader id `(term, node_id)` first and
//! by `committed` second, so `(T, 2, uncommitted) > (T, 1, committed)`: the
//! order is not "same term: committed wins". Two votes are never
//! incomparable in this build, but [`adopt_vote`] still treats an
//! incomparable maximum as "ask again later" instead of picking one.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;

use openraft::{LogId, Vote};
use serde::{Deserialize, Serialize};

use crate::NodeId;
use crate::forward::ForwardError;

/// What a node reports about its durable Raft state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct NodeStatus {
    /// The persisted vote.
    pub vote: Option<Vote<NodeId>>,
    /// The id of the last log entry (or of the purge point, if the log is
    /// empty above it).
    pub last_log_id: Option<LogId<NodeId>>,
    /// The persisted commit hint.
    pub committed: Option<LogId<NodeId>>,
    /// Any Raft state at all (a vote, log entries or a purge point).
    pub has_state: bool,
}

impl NodeStatus {
    /// The node belongs to a cluster that has had a leader: it holds a
    /// committed vote, a log entry beyond the bootstrap membership (index
    /// 0), or a commit hint. A node that only ran `--cluster-init` (the
    /// membership at index 0 and its own uncommitted candidacy, see
    /// openraft's `Engine::initialize`) is not established.
    pub fn established(&self) -> bool {
        self.vote.is_some_and(|v| v.is_committed())
            || self.last_log_id.is_some_and(|l| l.index >= 1)
            || self.committed.is_some()
    }
}

/// Answers status probes (implemented by the log store).
pub trait StatusSource: Send + Sync + 'static {
    fn status(&self) -> NodeStatus;
}

/// The client side of status probes (the TCP network and the simulated
/// one).
pub trait StatusTransport: Clone + Send + Sync + 'static {
    fn status(
        &self,
        target: NodeId,
    ) -> impl Future<Output = Result<NodeStatus, ForwardError>> + Send;
}

/// Asks every node of `peers` other than `id` for its status, at once.
/// Returns the answers that arrived (errors are logged at debug level).
pub async fn probe<T: StatusTransport>(
    net: &T,
    id: NodeId,
    peers: &BTreeSet<NodeId>,
) -> BTreeMap<NodeId, NodeStatus> {
    let mut asks = tokio::task::JoinSet::new();
    for &p in peers.iter().filter(|&&p| p != id) {
        let net = net.clone();
        asks.spawn(async move { (p, net.status(p).await) });
    }
    let mut got = BTreeMap::new();
    while let Some(done) = asks.join_next().await {
        match done {
            Ok((p, Ok(s))) => {
                got.insert(p, s);
            }
            Ok((p, Err(e))) => tracing::debug!(peer = p, "status probe failed: {e}"),
            Err(e) => tracing::debug!("status probe task failed: {e}"),
        }
    }
    got
}

/// Answers from other nodes needed before this node may act on them: a
/// majority of the cluster, `⌊n/2⌋ + 1`, counted among the *other* nodes
/// (for n = 3 both others, for n = 5 three of the four).
pub fn quorum(n: usize) -> usize {
    n / 2 + 1
}

/// The outcome of [`adopt_vote`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Adopt {
    /// Not enough answers yet.
    TooFew,
    /// The highest vote among the answers and `local` (`None`: nobody has
    /// voted yet).
    Vote(Option<Vote<NodeId>>),
    /// Two votes at the top are incomparable: ask again later.
    Incomparable,
}

/// The vote a rejoining node persists before it starts Raft: the highest
/// among the answers of at least [`quorum`]`(n)` other nodes and its own
/// persisted vote `local` (never lower than that).
pub fn adopt_vote(
    n: usize,
    answers: &BTreeMap<NodeId, NodeStatus>,
    local: Option<Vote<NodeId>>,
) -> Adopt {
    if answers.len() < quorum(n) {
        return Adopt::TooFew;
    }
    let mut best: Option<Vote<NodeId>> = local;
    for v in answers.values().filter_map(|s| s.vote) {
        best = match best {
            None => Some(v),
            Some(b) => match v.partial_cmp(&b) {
                Some(std::cmp::Ordering::Greater) => Some(v),
                Some(_) => Some(b),
                None => return Adopt::Incomparable,
            },
        };
    }
    // Every vote must be at or below the maximum (a vote incomparable with
    // the maximum was not caught above if it came before a larger one).
    if let Some(b) = best
        && answers
            .values()
            .filter_map(|s| s.vote)
            .chain(local)
            .any(|v| v.partial_cmp(&b).is_none())
    {
        return Adopt::Incomparable;
    }
    Adopt::Vote(best)
}

/// What a node started with `--cluster-init` and an empty data directory
/// does (see [`bootstrap_decision`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bootstrap {
    /// Initialize the membership (a fresh cluster).
    Initialize,
    /// A peer belongs to a cluster that has had a leader: rejoin it (a
    /// wiped node started with `--cluster-init` by mistake).
    Rejoin(NodeId),
    /// Not enough answers to decide: ask again later.
    Wait,
}

/// The `--cluster-init` decision for a cluster of `n` nodes from the
/// answers of the other nodes:
///
/// - any answer from an [established](NodeStatus::established) node:
///   rejoin;
/// - at least [`quorum`]`(n)` answers, all without any state: initialize.
///   (A node that ever led was elected by a quorum, at least
///   `quorum(n) - 1` of them other than this node, and each of those holds
///   a vote, so such a set of answers includes one with state);
/// - answers from *every* other node, some of them bootstrap-only (the
///   membership at index 0 and an uncommitted vote: an initial node that
///   initialized first): initialize too. All of them are needed here, since
///   the voters of a leader hold its vote uncommitted until its first
///   append, which looks the same; only the leader itself shows a committed
///   vote and an entry at index 1, and with every node answering it is
///   among them. Without this case, bootstrapping would deadlock once one
///   initial node initialized before the others asked: they would all
///   rejoin, and nobody could elect the one that initialized.
/// - otherwise wait.
///
/// For `n = 1` there is nobody to ask: initialize.
pub fn bootstrap_decision(n: usize, answers: &BTreeMap<NodeId, NodeStatus>) -> Bootstrap {
    if let Some((&p, _)) = answers.iter().find(|(_, s)| s.established()) {
        return Bootstrap::Rejoin(p);
    }
    if n <= 1 {
        return Bootstrap::Initialize;
    }
    let all_empty = answers.values().all(|s| !s.has_state);
    if (all_empty && answers.len() >= quorum(n)) || answers.len() + 1 >= n {
        return Bootstrap::Initialize;
    }
    Bootstrap::Wait
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::CommittedLeaderId;

    fn st(vote: Option<Vote<NodeId>>, last: Option<u64>) -> NodeStatus {
        NodeStatus {
            vote,
            last_log_id: last.map(|i| LogId::new(CommittedLeaderId::new(0, 0), i)),
            committed: None,
            has_state: vote.is_some() || last.is_some(),
        }
    }

    fn answers(v: &[(NodeId, NodeStatus)]) -> BTreeMap<NodeId, NodeStatus> {
        v.iter().copied().collect()
    }

    #[test]
    fn vote_order_is_openrafts() {
        // Leader id first, then `committed` (no single-term-leader).
        assert!(Vote::<NodeId>::new(3, 2) > Vote::new_committed(3, 1));
        assert!(Vote::<NodeId>::new_committed(3, 1) > Vote::new(3, 1));
        assert!(Vote::<NodeId>::new(4, 1) > Vote::new_committed(3, 3));
        assert!(
            Vote::<NodeId>::new(3, 1)
                .partial_cmp(&Vote::new(3, 2))
                .is_some()
        );
    }

    #[test]
    fn adopt_needs_a_quorum_of_others_and_takes_the_max() {
        let a = answers(&[(2, st(Some(Vote::new_committed(5, 2)), Some(9)))]);
        assert_eq!(adopt_vote(3, &a, None), Adopt::TooFew);
        let a = answers(&[
            (2, st(Some(Vote::new_committed(5, 2)), Some(9))),
            (3, st(Some(Vote::new(4, 3)), Some(7))),
        ]);
        assert_eq!(
            adopt_vote(3, &a, None),
            Adopt::Vote(Some(Vote::new_committed(5, 2)))
        );
        // Never below the local vote.
        assert_eq!(
            adopt_vote(3, &a, Some(Vote::new(6, 1))),
            Adopt::Vote(Some(Vote::new(6, 1)))
        );
        // Same term: the higher node id wins over `committed`.
        let a = answers(&[
            (2, st(Some(Vote::new_committed(5, 2)), Some(9))),
            (3, st(Some(Vote::new(5, 3)), Some(9))),
        ]);
        assert_eq!(adopt_vote(3, &a, None), Adopt::Vote(Some(Vote::new(5, 3))));
        // n = 5: three of the four others.
        let three = answers(&[
            (2, st(None, None)),
            (3, st(Some(Vote::new(1, 3)), None)),
            (4, st(None, None)),
        ]);
        assert_eq!(
            adopt_vote(5, &three, None),
            Adopt::Vote(Some(Vote::new(1, 3)))
        );
        let two = answers(&[(2, st(None, None)), (4, st(None, None))]);
        assert_eq!(adopt_vote(5, &two, None), Adopt::TooFew);
        // Nobody voted yet.
        let none = answers(&[(2, st(None, None)), (3, st(None, None))]);
        assert_eq!(adopt_vote(3, &none, None), Adopt::Vote(None));
    }

    #[test]
    fn bootstrap_decisions() {
        let empty = st(None, None);
        let init_only = st(Some(Vote::new(1, 1)), Some(0));
        let leader = NodeStatus {
            vote: Some(Vote::new_committed(1, 1)),
            ..init_only
        };
        let follower = st(Some(Vote::new_committed(1, 1)), Some(0));
        let with_entries = st(Some(Vote::new(2, 3)), Some(4));

        assert_eq!(
            bootstrap_decision(1, &BTreeMap::new()),
            Bootstrap::Initialize
        );
        assert_eq!(
            bootstrap_decision(3, &answers(&[(2, empty)])),
            Bootstrap::Wait
        );
        assert_eq!(
            bootstrap_decision(3, &answers(&[(2, empty), (3, empty)])),
            Bootstrap::Initialize
        );
        // One initial node initialized first: the others follow.
        assert_eq!(
            bootstrap_decision(3, &answers(&[(1, init_only), (3, empty)])),
            Bootstrap::Initialize
        );
        assert_eq!(
            bootstrap_decision(3, &answers(&[(1, init_only)])),
            Bootstrap::Wait
        );
        // n = 5: bootstrap-only state needs every other node.
        assert_eq!(
            bootstrap_decision(5, &answers(&[(1, init_only), (3, empty), (4, empty)])),
            Bootstrap::Wait
        );
        assert_eq!(
            bootstrap_decision(
                5,
                &answers(&[(1, init_only), (3, empty), (4, empty), (5, init_only)])
            ),
            Bootstrap::Initialize
        );
        assert_eq!(
            bootstrap_decision(5, &answers(&[(2, empty), (3, empty), (4, empty)])),
            Bootstrap::Initialize
        );
        // Established peers: rejoin, whatever else answered.
        for s in [leader, follower, with_entries] {
            assert_eq!(
                bootstrap_decision(3, &answers(&[(1, s)])),
                Bootstrap::Rejoin(1)
            );
        }
        let hinted = NodeStatus {
            committed: Some(LogId::new(CommittedLeaderId::new(1, 1), 0)),
            ..init_only
        };
        assert_eq!(
            bootstrap_decision(3, &answers(&[(2, hinted)])),
            Bootstrap::Rejoin(2)
        );
    }
}
