//! Node status probes (wire protocol version 3) and the startup decisions
//! built on them: safe rejoin and `--cluster-init` (docs/DESIGN.md §8).
//!
//! A node answers [`crate::wire::RpcRequest::Status`] from its log store
//! ([`StatusSource`]), whether or not its Raft is running, so a node that
//! has not started Raft yet (a rejoining node probing its peers, or an
//! initial node deciding whether to bootstrap) can be asked too.
//!
//! [`crate::wire::RpcRequest::StatusEx`] (protocol version 4) answers the same
//! durable state plus the node's view of the membership ([`NodeStatusEx`]),
//! in the same situations.
//!
//! # Vote order
//!
//! Comparisons use openraft's own `PartialOrd` on `Vote`: without its
//! `single-term-leader` feature a vote is ordered by leader id `(term,
//! node_id)` first and `committed` second (docs/DESIGN.md §8 "Why rejoin is
//! safe"). [`adopt_vote`] still treats an incomparable maximum as "ask again
//! later" instead of picking one.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;

use openraft::{BasicNode, LogId, Membership, StoredMembership, Vote};
use serde::{Deserialize, Serialize};

use crate::NodeId;
use crate::forward::ForwardError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct NodeStatus {
    pub vote: Option<Vote<NodeId>>,
    /// The id of the last log entry (or of the purge point, if the log is
    /// empty above it).
    pub last_log_id: Option<LogId<NodeId>>,
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

/// A node's view of the cluster membership (protocol version 4). Mirrors
/// openraft's `Membership`: voter sets (`configs`, two in a joint
/// configuration) and every node, voters and learners, with its address.
/// Decoding is bounded like the membership of a log entry (see
/// [`crate::wire`]).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct MembershipView {
    /// The membership entry's log id (`None`: no membership).
    pub log_id: Option<LogId<NodeId>>,
    /// Whether that entry is committed, as far as this node knows.
    pub committed: bool,
    #[serde(deserialize_with = "crate::wire::bounded_voter_sets")]
    pub configs: Vec<BTreeSet<NodeId>>,
    #[serde(deserialize_with = "crate::wire::bounded_node_addrs")]
    pub nodes: BTreeMap<NodeId, String>,
}

impl MembershipView {
    pub fn new(
        log_id: Option<LogId<NodeId>>,
        m: &Membership<NodeId, BasicNode>,
        committed: bool,
    ) -> Self {
        MembershipView {
            log_id,
            committed,
            configs: m.get_joint_config().clone(),
            nodes: m.nodes().map(|(&id, n)| (id, n.addr.clone())).collect(),
        }
    }

    pub fn from_stored(m: &StoredMembership<NodeId, BasicNode>, committed: bool) -> Self {
        Self::new(*m.log_id(), m.membership(), committed)
    }

    pub fn is_joint(&self) -> bool {
        self.configs.len() > 1
    }

    /// Voters of every config (both halves of a joint configuration).
    pub fn voters(&self) -> BTreeSet<NodeId> {
        self.configs.iter().flatten().copied().collect()
    }

    /// Nodes that are in no voter set.
    pub fn learners(&self) -> BTreeSet<NodeId> {
        let voters = self.voters();
        self.nodes
            .keys()
            .filter(|id| !voters.contains(id))
            .copied()
            .collect()
    }

    pub fn is_member(&self, id: NodeId) -> bool {
        self.nodes.contains_key(&id)
    }
}

/// The answer to a [`crate::wire::RpcRequest::StatusEx`] probe: the durable
/// state of [`NodeStatus`] plus what the node knows about the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct NodeStatusEx {
    pub status: NodeStatus,
    /// Raft runs on the node; otherwise the answer comes from its storage
    /// alone (a node still probing its peers at startup), so `leader` is
    /// unknown and `term` is that of its vote.
    pub raft_running: bool,
    /// The node rejoins after data loss and does not vote yet.
    pub rejoining: bool,
    pub term: u64,
    pub leader: Option<NodeId>,
    pub last_applied: Option<LogId<NodeId>>,
    /// The highest node id this node knows was ever a member: the applied
    /// record (`SmMeta::highest_member`) or an id of `membership`, whichever
    /// is higher (0 if none). Ids at or below it are never reused.
    pub highest_member: NodeId,
    /// The effective membership (the latest in the node's log, committed
    /// or not).
    pub membership: MembershipView,
}

impl NodeStatusEx {
    /// What storage alone tells: no membership, Raft not running.
    pub fn from_status(status: NodeStatus) -> Self {
        NodeStatusEx {
            term: status.vote.map_or(0, |v| v.leader_id().term),
            status,
            ..NodeStatusEx::default()
        }
    }
}

/// Answers status probes (implemented by the log store, and by the server's
/// richer source).
pub trait StatusSource: Send + Sync + 'static {
    fn status(&self) -> NodeStatus;

    /// The default knows only the durable state ([`NodeStatusEx::from_status`]).
    fn status_ex(&self) -> NodeStatusEx {
        NodeStatusEx::from_status(self.status())
    }
}

pub trait StatusTransport: Clone + Send + Sync + 'static {
    fn status(
        &self,
        target: NodeId,
    ) -> impl Future<Output = Result<NodeStatus, ForwardError>> + Send;

    fn status_ex(
        &self,
        target: NodeId,
    ) -> impl Future<Output = Result<NodeStatusEx, ForwardError>> + Send;
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

/// A majority of `n` voters, `⌊n/2⌋ + 1`: the size of every election and
/// commit quorum of a uniform configuration. `--cluster-init` waits for this
/// many answers from the other initial voters (docs/DESIGN.md §8
/// "Bootstrap").
pub fn quorum(n: usize) -> usize {
    n / 2 + 1
}

/// Answers from *other* voters of an `n`-voter configuration a rejoining
/// node needs: `n - quorum(n) + 1`, the fewest that meet every quorum of the
/// configuration (`|P| + quorum(n) > n`), even one that contains the
/// rejoining node itself (docs/DESIGN.md §8 "Why rejoin is safe", the
/// bound). n = 1: 1, out of no other voter (never possible); n = 2: 1;
/// n = 3: 2; n = 4: 2; n = 5: 3. (n = 0, no voters at all: 1, never
/// reached since [`startup_decision`] waits on an empty voter set.)
pub fn rejoin_answers(n: usize) -> usize {
    n.saturating_sub(quorum(n)) + 1
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Adopt {
    TooFew,
    /// The highest vote among the answers and `local` (`None`: nobody has
    /// voted yet).
    Vote(Option<Vote<NodeId>>),
    /// Two votes at the top are incomparable: ask again later. Defensive:
    /// unreachable with the leader id this crate uses (openraft's
    /// `leader_id_adv` derives a total `Ord`, so `Vote::partial_cmp` always
    /// answers); only the `single-term-leader` build orders votes partially.
    Incomparable,
}

/// The vote a rejoining node persists before it starts Raft: the highest
/// among the answers of at least [`rejoin_answers`]`(n)` other voters of an
/// `n`-voter configuration and its own persisted vote `local` (never lower
/// than that).
pub fn adopt_vote(
    n: usize,
    answers: &BTreeMap<NodeId, NodeStatus>,
    local: Option<Vote<NodeId>>,
) -> Adopt {
    if answers.len() < rejoin_answers(n) {
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

/// Asks every node of `targets` other than `id` for its [`NodeStatusEx`], at
/// once. Returns the answers that arrived and, for the log, why the others
/// did not.
pub async fn probe_ex<T: StatusTransport>(
    net: &T,
    id: NodeId,
    targets: &BTreeSet<NodeId>,
) -> (BTreeMap<NodeId, NodeStatusEx>, BTreeMap<NodeId, String>) {
    let mut asks = tokio::task::JoinSet::new();
    for &p in targets.iter().filter(|&&p| p != id) {
        let net = net.clone();
        asks.spawn(async move { (p, net.status_ex(p).await) });
    }
    let mut got = BTreeMap::new();
    let mut failed = BTreeMap::new();
    while let Some(done) = asks.join_next().await {
        match done {
            Ok((p, Ok(s))) => {
                got.insert(p, s);
            }
            Ok((p, Err(e))) => {
                tracing::debug!(peer = p, "status probe failed: {e}");
                failed.insert(p, e.to_string());
            }
            Err(e) => tracing::debug!("status probe task failed: {e}"),
        }
    }
    (got, failed)
}

/// What a node without usable Raft state (an empty data directory, or the
/// rejoin marker) does, decided by [`startup_decision`] from the
/// [`NodeStatusEx`] answers of the other nodes (docs/DESIGN.md §8 "Startup
/// modes" and "Why rejoin is safe").
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Startup {
    /// Not decided yet: ask again. `fresh`: discard the answers gathered so
    /// far first (they cannot become sufficient by adding more).
    Wait { reason: String, fresh: bool },
    /// This id is in no membership and was never one: wait (asking again)
    /// until the operator adds it; the decision then becomes `Rejoin`.
    Join {
        voters: BTreeSet<NodeId>,
        highest_member: NodeId,
    },
    /// This id is a voter or learner of the current membership: persist
    /// `vote` (the highest vote of [`rejoin_answers`] current voters other
    /// than this node and not rejoining, and never below its own) before
    /// Raft starts.
    Rejoin {
        vote: Option<Vote<NodeId>>,
        membership: MembershipView,
    },
    /// Never start: the id was removed (or skipped: ids at or below the
    /// highest member id are never readmitted).
    Refuse(String),
}

/// The membership a node without state takes as the cluster's current one:
/// the committed membership with the highest log index among the answers
/// (committed membership entries all lie on the one committed log, so their
/// indexes order them). `None`: no answer knows a committed membership.
pub fn learned_membership(answers: &BTreeMap<NodeId, NodeStatusEx>) -> Option<&MembershipView> {
    answers
        .values()
        .map(|a| &a.membership)
        .filter(|m| m.committed && m.log_id.is_some() && !m.nodes.is_empty())
        .max_by_key(|m| m.log_id.map(|l| l.index))
}

/// The startup decision of node `id` without Raft state, from the
/// [`NodeStatusEx`] answers of other nodes (seeds and members) and its own
/// persisted vote `local` (a restart with the rejoin marker). In order:
///
/// 1. `M` = [`learned_membership`]; none: wait. `M` joint: wait (a rejoin
///    would need a quorum of both halves; a joint configuration is left
///    quickly).
/// 2. `V` = the voters of `M`. If `id` is a voter and `V \ {id}` has fewer
///    than [`rejoin_answers`]`(|V|)` nodes, wait (a single voter: nobody can
///    vouch for this node's lost votes; not a refusal, since `M` may come
///    from a stale node).
/// 3. Any answer from `V \ {id}` reporting a membership at a higher index
///    than `M` (it may be committed without that node knowing), rejoining
///    or not: wait. `P` = the answers from `V \ {id}` that are not
///    rejoining themselves and whose membership is `M` itself (same log id);
///    fewer than [`rejoin_answers`]`(|V|)`: wait. So `M` was the latest
///    committed membership when the earliest answer of `P` was given (a
///    later committed one needed a quorum of `V`, which meets `P`, and a
///    node of `P` holds every entry it acknowledged).
/// 4. `id` is a node of `M` (voter or learner): rejoin with the highest vote
///    of `P` and `local` ([`adopt_vote`]; an incomparable top, or a
///    committed vote naming this node, waits with fresh answers).
/// 5. Otherwise, any answer whose membership is newer than `M` and lists
///    `id`: wait (it is being added). Else any answer holding `M` with
///    `highest_member >= id`: refuse (removed or skipped).
/// 6. Otherwise, if some answer holding `M` has applied up to `M` (its
///    applied record of the highest member covers every committed membership
///    up to `M`): join. Else wait.
///
/// Why this is safe: docs/DESIGN.md §8 "Why rejoin is safe".
pub fn startup_decision(
    id: NodeId,
    answers: &BTreeMap<NodeId, NodeStatusEx>,
    local: Option<Vote<NodeId>>,
) -> Startup {
    let wait = |reason: String| Startup::Wait {
        reason,
        fresh: false,
    };
    let Some(m) = learned_membership(answers) else {
        return wait("no node that answered knows a committed membership".into());
    };
    let Some(m_index) = m.log_id.map(|l| l.index) else {
        return wait("no node that answered knows a committed membership".into());
    };
    if m.is_joint() {
        return wait(format!(
            "the membership at index {m_index} is a joint configuration {:?}: waiting until it \
             is uniform",
            m.configs
        ));
    }
    let voters = m.voters();
    if voters.is_empty() {
        // Never a real membership (openraft requires a voter); only a
        // malformed answer gets here.
        return wait(format!("the membership at index {m_index} has no voters"));
    }
    let others: BTreeSet<NodeId> = voters.iter().copied().filter(|&v| v != id).collect();
    let need = rejoin_answers(voters.len());
    if voters.contains(&id) && others.len() < need {
        // Not a refusal: `M` may come from a stale node, and the nodes it
        // names may report a newer membership. (Only a single voter gets
        // here: it cannot be removed either, since that needs it as leader.)
        return wait(format!(
            "this node is the only voter {voters:?} (membership at index {m_index}): if this \
             is the current membership, it cannot rejoin, since a node that lost its data \
             needs at least {need} other voter to vouch for the votes it may have granted, and \
             no other node holds the cluster's data; restore its data directory"
        ));
    }
    let from_voters: BTreeMap<NodeId, &NodeStatusEx> = answers
        .iter()
        .filter(|(n, _)| others.contains(n))
        .map(|(&n, a)| (n, a))
        .collect();
    if let Some((n, a)) = from_voters
        .iter()
        .find(|(_, a)| a.membership.log_id.is_some_and(|l| l.index > m_index))
    {
        return wait(format!(
            "voter {n} reports a newer membership (index {}, committed: {}) than the committed \
             one at index {m_index}: a membership change is in progress",
            a.membership.log_id.map_or(0, |l| l.index),
            a.membership.committed
        ));
    }
    // Only voters that hold `M` itself count: each then holds every entry up
    // to it, so its vote is at least that of the leader that sent it those
    // (DESIGN §8 "Why rejoin is safe", case B of Claim 2). A voter that is
    // rejoining itself does not count: it may lack entries it acknowledged
    // before its own data loss (Claim 1), and in discovery it has no vote.
    let p: BTreeMap<NodeId, &NodeStatusEx> = from_voters
        .into_iter()
        .filter(|(_, a)| !a.rejoining && a.membership.log_id == m.log_id)
        .collect();
    if p.len() < need {
        return wait(format!(
            "{} of the {need} answers needed from the current voters {others:?} holding the \
             membership at index {m_index} and not rejoining themselves",
            p.len()
        ));
    }
    if m.is_member(id) {
        let statuses: BTreeMap<NodeId, NodeStatus> =
            p.iter().map(|(&n, a)| (n, a.status)).collect();
        return match adopt_vote(voters.len(), &statuses, local) {
            Adopt::Vote(Some(v)) if v.is_committed() && v.leader_id().voted_for() == Some(id) => {
                // The others still follow this node's previous life as their
                // leader: wait until they elect another one (never start with
                // a committed vote naming this node).
                Startup::Wait {
                    reason: format!("the highest vote ({v}) is this node's own leadership"),
                    fresh: true,
                }
            }
            Adopt::Vote(vote) => Startup::Rejoin {
                vote,
                membership: m.clone(),
            },
            Adopt::TooFew => wait("too few answers".into()),
            Adopt::Incomparable => Startup::Wait {
                reason: "the highest votes are incomparable".into(),
                fresh: true,
            },
        };
    }
    if let Some((n, a)) = answers.iter().find(|(_, a)| {
        a.membership.is_member(id) && a.membership.log_id.is_some_and(|l| l.index > m_index)
    }) {
        // Being added: that entry is newer than `M` and not known to be
        // committed yet, and it raises that node's highest member id to this
        // one. (An older membership listing this id is history: a removal.)
        return wait(format!(
            "node {n} lists this node in a membership at index {} that is not known to be \
             committed yet",
            a.membership.log_id.map_or(0, |l| l.index)
        ));
    }
    // From here on only nodes holding `M` itself: a non-voter holding a
    // newer, uncommitted membership has its highest member id raised by it.
    let holders: Vec<&NodeStatusEx> = answers
        .values()
        .filter(|a| a.membership.log_id == m.log_id)
        .collect();
    let highest = holders.iter().map(|a| a.highest_member).max().unwrap_or(0);
    if highest >= id {
        return Startup::Refuse(format!(
            "node id {id} is not a member, and ids up to {highest} have been used: node ids are \
             never reused (a removed node joins again only under a new id above {highest})"
        ));
    }
    if holders
        .iter()
        .any(|a| a.last_applied.is_some_and(|l| l.index >= m_index))
    {
        Startup::Join {
            voters,
            highest_member: highest,
        }
    } else {
        wait(format!(
            "no node that answered has applied the membership at index {m_index} yet, so the \
             highest member id it reports may be stale"
        ))
    }
}

/// `Some(reason)` if the answers of other nodes confirm that node `id`,
/// which has Raft state, was removed from the cluster: [`startup_decision`]
/// refuses its id, and at least [`quorum`]`(n)` of the `n` voters of the
/// membership it learned (a uniform one: a joint configuration waits) report
/// exactly that committed membership and are not rejoining. Local evidence
/// is never an input. Why a node that is still a member never gets here:
/// docs/DESIGN.md §8 "A removed node stops by itself".
pub fn removal_confirmed(id: NodeId, answers: &BTreeMap<NodeId, NodeStatusEx>) -> Option<String> {
    let Startup::Refuse(reason) = startup_decision(id, answers, None) else {
        return None;
    };
    let m = learned_membership(answers)?;
    let voters = m.voters();
    let holding = answers
        .iter()
        .filter(|(n, a)| voters.contains(n) && !a.rejoining && a.membership.log_id == m.log_id)
        .count();
    (holding >= quorum(voters.len())).then_some(reason)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bootstrap {
    Initialize,
    /// A peer belongs to a cluster that has had a leader: rejoin it (a
    /// wiped node started with `--cluster-init` by mistake).
    Rejoin(NodeId),
    Wait,
}

/// The `--cluster-init` decision for a cluster of `n` nodes from the answers
/// of the other nodes: rejoin if any answer is
/// [established](NodeStatus::established); initialize on at least
/// [`quorum`]`(n)` answers without any state, or on answers from *every*
/// other node when some are bootstrap-only (an initial node that initialized
/// first); otherwise wait. `n = 1` initializes. Why each rule is safe, and why
/// the second is needed: docs/DESIGN.md §8 "Bootstrap".
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

    fn view(
        index: u64,
        committed: bool,
        configs: &[&[NodeId]],
        learners: &[NodeId],
    ) -> MembershipView {
        let configs: Vec<BTreeSet<NodeId>> = configs
            .iter()
            .map(|c| c.iter().copied().collect())
            .collect();
        let nodes = configs
            .iter()
            .flatten()
            .chain(learners)
            .map(|&n| (n, format!("10.0.0.{n}:11400")))
            .collect();
        MembershipView {
            log_id: Some(LogId::new(CommittedLeaderId::new(1, 1), index)),
            committed,
            configs,
            nodes,
        }
    }

    /// A member's answer: vote, membership, applied index.
    fn ex(vote: Option<Vote<NodeId>>, m: &MembershipView, applied: u64) -> NodeStatusEx {
        NodeStatusEx {
            status: st(vote, Some(applied)),
            raft_running: true,
            last_applied: Some(LogId::new(CommittedLeaderId::new(1, 1), applied)),
            highest_member: m.nodes.keys().copied().max().unwrap_or(0),
            membership: m.clone(),
            ..NodeStatusEx::default()
        }
    }

    fn exs(v: &[(NodeId, NodeStatusEx)]) -> BTreeMap<NodeId, NodeStatusEx> {
        v.iter().cloned().collect()
    }

    fn waits(d: &Startup) -> bool {
        matches!(d, Startup::Wait { .. })
    }

    #[test]
    fn startup_rejoin_uses_the_current_voters() {
        // 3 -> 4 voters: {1,2,3,4}, node 4 rejoins: rejoin_answers(4) = 2
        // answers among {1,2,3}.
        let m = view(9, true, &[&[1, 2, 3, 4]], &[]);
        let v = Vote::new_committed(3, 1);
        let one = exs(&[(1, ex(Some(v), &m, 9))]);
        let d = startup_decision(4, &one, None);
        assert!(
            matches!(&d, Startup::Wait { reason, .. } if reason.contains("1 of the 2")),
            "{d:?}"
        );
        let two = exs(&[(1, ex(Some(v), &m, 9)), (2, ex(Some(v), &m, 9))]);
        assert_eq!(
            startup_decision(4, &two, None),
            Startup::Rejoin {
                vote: Some(v),
                membership: m.clone()
            }
        );
        let mut three = two.clone();
        three.insert(3, ex(Some(Vote::new(4, 3)), &m, 9));
        assert_eq!(
            startup_decision(4, &three, None),
            Startup::Rejoin {
                vote: Some(Vote::new(4, 3)),
                membership: m.clone()
            }
        );
        // A stale seed (node 7, removed, an old membership) adds nothing:
        // the newest committed membership wins.
        let old = view(2, true, &[&[1, 2, 7]], &[]);
        let mut with_old = three.clone();
        with_old.insert(7, ex(Some(Vote::new_committed(9, 7)), &old, 2));
        assert_eq!(
            startup_decision(4, &with_old, None),
            Startup::Rejoin {
                vote: Some(Vote::new(4, 3)),
                membership: m.clone()
            }
        );
        // Only the stale seed answered: it names voters to ask, but they
        // have not answered: wait (never adopt from non-voters).
        let only_old = exs(&[(7, ex(Some(Vote::new_committed(9, 7)), &old, 2))]);
        let d = startup_decision(1, &only_old, None);
        assert!(waits(&d), "{d:?}");
        // The local vote is a floor.
        assert_eq!(
            startup_decision(4, &three, Some(Vote::new(8, 4))),
            Startup::Rejoin {
                vote: Some(Vote::new(8, 4)),
                membership: m.clone()
            }
        );
    }

    #[test]
    fn startup_waits_for_a_committed_uniform_membership() {
        assert!(waits(&startup_decision(1, &BTreeMap::new(), None)));
        // Nobody knows a membership yet (a cluster not bootstrapped).
        let none = exs(&[(2, NodeStatusEx::default()), (3, NodeStatusEx::default())]);
        assert!(waits(&startup_decision(1, &none, None)));
        // Joint: wait.
        let joint = view(5, true, &[&[1, 2, 3], &[1, 2, 4]], &[]);
        let a = exs(&[
            (1, ex(None, &joint, 5)),
            (2, ex(None, &joint, 5)),
            (4, ex(None, &joint, 5)),
        ]);
        let d = startup_decision(3, &a, None);
        assert!(
            matches!(&d, Startup::Wait { reason, .. } if reason.contains("joint")),
            "{d:?}"
        );
        // A malformed answer: committed, nodes but no voters: wait.
        let empty = MembershipView {
            configs: vec![],
            ..view(6, true, &[], &[1, 2])
        };
        let a = exs(&[(1, ex(None, &empty, 6)), (2, ex(None, &empty, 6))]);
        let d = startup_decision(1, &a, None);
        assert!(
            matches!(&d, Startup::Wait { reason, .. } if reason.contains("no voters")),
            "{d:?}"
        );
        // Uncommitted only (as far as anyone knows): wait.
        let unc = view(5, false, &[&[1, 2, 3]], &[]);
        let a = exs(&[(1, ex(None, &unc, 4)), (2, ex(None, &unc, 4))]);
        assert!(waits(&startup_decision(3, &a, None)));
        // A voter of M reports a newer (uncommitted) membership: a change is
        // in flight, wait, whatever this node's role.
        let m = view(5, true, &[&[1, 2, 3]], &[]);
        let newer = view(8, false, &[&[1, 2, 3], &[1, 2, 3, 4]], &[]);
        let a = exs(&[(1, ex(None, &newer, 7)), (2, ex(None, &m, 7))]);
        let d = startup_decision(3, &a, None);
        assert!(
            matches!(&d, Startup::Wait { reason, .. } if reason.contains("newer")),
            "{d:?}"
        );
        let d = startup_decision(4, &a, None);
        assert!(waits(&d), "{d:?}");
    }

    #[test]
    fn startup_waits_without_a_quorum_of_other_voters() {
        // The answer may be stale (the nodes it names may know a newer
        // membership), so this waits, saying what it means if it is not.
        let one = view(1, true, &[&[1]], &[]);
        let d = startup_decision(1, &exs(&[(2, ex(None, &one, 1))]), None);
        assert!(
            matches!(&d, Startup::Wait { reason, .. } if reason.contains("cannot rejoin")),
            "{d:?}"
        );
        // Two voters: rejoin_answers(2) = 1, the other voter (whose vote
        // every quorum of {1, 2} includes).
        let two = view(4, true, &[&[1, 2]], &[]);
        let v = Vote::new_committed(2, 1);
        assert_eq!(
            startup_decision(2, &exs(&[(1, ex(Some(v), &two, 4))]), None),
            Startup::Rejoin {
                vote: Some(v),
                membership: two.clone()
            }
        );
        // Node 1 lags ({1, 2} at index 4), node 3 knows the current
        // {1, 2, 3}: the target set grows, and node 1 counts only once it
        // holds that membership itself.
        let three = view(9, true, &[&[1, 2, 3]], &[]);
        let a = exs(&[(1, ex(None, &two, 4)), (3, ex(None, &three, 9))]);
        let d = startup_decision(2, &a, None);
        assert!(
            matches!(&d, Startup::Wait { reason, .. } if reason.contains("1 of the 2")),
            "{d:?}"
        );
        let a = exs(&[(1, ex(None, &three, 9)), (3, ex(None, &three, 9))]);
        assert!(matches!(
            startup_decision(2, &a, None),
            Startup::Rejoin { .. }
        ));
    }

    #[test]
    fn rejoin_answers_meet_every_quorum() {
        let table: Vec<usize> = (1..=7).map(rejoin_answers).collect();
        assert_eq!(table, [1, 1, 2, 2, 3, 3, 4]);
        // Total: no underflow on an empty voter set.
        assert_eq!(rejoin_answers(0), 1);
        for n in 1..=9 {
            let p = rejoin_answers(n);
            // Every quorum of n voters meets every set of p voters...
            assert!(p + quorum(n) > n, "n = {n}");
            // ...and p is the fewest that do.
            assert!(p - 1 + quorum(n) <= n, "n = {n}");
            // Never more than a quorum.
            assert!(p <= quorum(n), "n = {n}");
        }
        // adopt_vote counts the same bound.
        let a = answers(&[
            (1, st(Some(Vote::new(2, 1)), Some(3))),
            (2, st(Some(Vote::new_committed(3, 2)), Some(4))),
        ]);
        assert_eq!(
            adopt_vote(4, &a, None),
            Adopt::Vote(Some(Vote::new_committed(3, 2)))
        );
        let one = answers(&[(1, st(Some(Vote::new(2, 1)), Some(3)))]);
        assert_eq!(adopt_vote(4, &one, None), Adopt::TooFew);
        assert_eq!(
            adopt_vote(2, &one, None),
            Adopt::Vote(Some(Vote::new(2, 1)))
        );
        assert_eq!(adopt_vote(1, &BTreeMap::new(), None), Adopt::TooFew);
    }

    /// The review counterexample (H1), at the level of the decision: five
    /// voters {1..5} hold `M` at index 10. Voters 2 and 4 were partitioned
    /// away with a stale vote; voter 3 (F) lost its data and is part-way
    /// through re-replication: it holds `M` (index 10) again and the stale
    /// vote its own rejoin adopted, but not the entries up to 60 it had
    /// acknowledged, with 1 and 5 (R), before its wipe, under leader 1's
    /// vote. R, wiped later, sees {2, 3, 4}: counting F, that is three
    /// answers holding `M` and R would adopt the stale vote, letting the
    /// stale leader of that vote commit another entry at index 60 with R's
    /// acknowledgement. F does not count, so R waits until 1 answers.
    #[test]
    fn startup_ignores_rejoining_voters_for_the_vote() {
        let m = view(10, true, &[&[1, 2, 3, 4, 5]], &[]);
        let stale = Vote::new_committed(3, 2);
        let current = Vote::new_committed(5, 1);
        let f = NodeStatusEx {
            rejoining: true,
            ..ex(Some(stale), &m, 10)
        };
        let a = exs(&[
            (2, ex(Some(stale), &m, 10)),
            (3, f.clone()),
            (4, ex(Some(stale), &m, 10)),
        ]);
        let d = startup_decision(5, &a, None);
        assert!(
            matches!(&d, Startup::Wait { reason, .. } if reason.contains("2 of the 3")),
            "{d:?}"
        );
        // Counting F, as before the fix, three answers would have adopted
        // the stale vote.
        let as_if_intact = exs(&[
            (2, ex(Some(stale), &m, 10)),
            (3, ex(Some(stale), &m, 10)),
            (4, ex(Some(stale), &m, 10)),
        ]);
        assert_eq!(
            startup_decision(5, &as_if_intact, None),
            Startup::Rejoin {
                vote: Some(stale),
                membership: m.clone()
            }
        );
        // Voter 1 (intact, it led the commits up to 60) answers: adopt its
        // vote.
        let mut with_1 = a.clone();
        with_1.insert(1, ex(Some(current), &m, 60));
        assert_eq!(
            startup_decision(5, &with_1, None),
            Startup::Rejoin {
                vote: Some(current),
                membership: m.clone()
            }
        );
        // A rejoining voter still counts towards the membership: one that
        // reports a newer membership makes R wait.
        let newer = view(70, false, &[&[1, 2, 3, 4, 5], &[1, 2, 3, 4]], &[5]);
        let mut with_newer = with_1.clone();
        with_newer.insert(
            3,
            NodeStatusEx {
                rejoining: true,
                ..ex(Some(current), &newer, 60)
            },
        );
        let d = startup_decision(5, &with_newer, None);
        assert!(
            matches!(&d, Startup::Wait { reason, .. } if reason.contains("newer")),
            "{d:?}"
        );
        // And its committed membership is learned like any other: the
        // newest committed one wins even from a rejoining node.
        let m2 = view(80, true, &[&[1, 2, 3, 4, 6]], &[]);
        let mut learned = with_1.clone();
        learned.insert(
            3,
            NodeStatusEx {
                rejoining: true,
                ..ex(Some(current), &m2, 80)
            },
        );
        assert_eq!(learned_membership(&learned), Some(&m2));
    }

    #[test]
    fn startup_join_and_learners() {
        // Growth 1 -> 2: voter {1}. Node 2, never a member: join, once an
        // answer has applied the membership.
        let m = view(1, true, &[&[1]], &[]);
        let lagging = NodeStatusEx {
            last_applied: Some(LogId::new(CommittedLeaderId::new(1, 1), 0)),
            ..ex(Some(Vote::new_committed(1, 1)), &m, 0)
        };
        let d = startup_decision(2, &exs(&[(1, lagging)]), None);
        assert!(waits(&d), "{d:?}");
        let a = exs(&[(1, ex(Some(Vote::new_committed(1, 1)), &m, 1))]);
        assert_eq!(
            startup_decision(2, &a, None),
            Startup::Join {
                voters: [1].into(),
                highest_member: 1
            }
        );
        // Added as a learner (committed): a learner rejoins, adopting the
        // vote of a quorum of the voters ({1}: one answer).
        let ml = view(2, true, &[&[1]], &[2]);
        let a = exs(&[(1, ex(Some(Vote::new_committed(1, 1)), &ml, 2))]);
        assert_eq!(
            startup_decision(2, &a, None),
            Startup::Rejoin {
                vote: Some(Vote::new_committed(1, 1)),
                membership: ml.clone()
            }
        );
        // The add is not known committed by a non-voter that lists it: wait,
        // and never take its raised highest member id for a removal.
        let unc = view(2, false, &[&[1, 3, 5]], &[6]);
        let m3 = view(1, true, &[&[1, 3, 5]], &[]);
        let a = exs(&[
            (1, ex(None, &m3, 1)),
            (3, ex(None, &m3, 1)),
            (6, ex(None, &unc, 1)),
        ]);
        let d = startup_decision(6, &a, None);
        assert!(waits(&d), "{d:?}");
    }

    #[test]
    fn startup_refuses_a_removed_or_skipped_id() {
        // Node 3 was removed: voters {1,2,4}, highest member 4.
        let m = view(12, true, &[&[1, 2, 4]], &[]);
        let a = exs(&[(1, ex(None, &m, 12)), (2, ex(None, &m, 12))]);
        let d = startup_decision(3, &a, None);
        assert!(
            matches!(&d, Startup::Refuse(r) if r.contains("never reused")),
            "{d:?}"
        );
        // The applied record may name a removed id above every member.
        let m = view(12, true, &[&[1, 2, 3]], &[]);
        let mut gone = ex(None, &m, 12);
        gone.highest_member = 5;
        let a = exs(&[(1, gone), (2, ex(None, &m, 12))]);
        assert!(matches!(startup_decision(5, &a, None), Startup::Refuse(_)));
        assert!(matches!(startup_decision(4, &a, None), Startup::Refuse(_)));
        assert!(matches!(
            startup_decision(6, &a, None),
            Startup::Join { .. }
        ));
        // A removed node still running reports its last membership, which
        // lists the removed id: history, not an add in progress.
        let stale = view(3, true, &[&[1, 2, 3]], &[]);
        let m = view(12, true, &[&[1, 2, 4]], &[]);
        let a = exs(&[
            (1, ex(None, &m, 12)),
            (2, ex(None, &m, 12)),
            (4, ex(None, &m, 12)),
            (3, ex(None, &stale, 3)),
        ]);
        let d = startup_decision(3, &a, None);
        assert!(matches!(&d, Startup::Refuse(_)), "{d:?}");
        // A learner (5) holding a newer, uncommitted membership that names
        // node 7 does not make the joiner 6 a skipped id.
        let m = view(12, true, &[&[1, 2, 4]], &[5]);
        let newer = view(13, false, &[&[1, 2, 4]], &[5, 7]);
        let a = exs(&[
            (1, ex(None, &m, 12)),
            (2, ex(None, &m, 12)),
            (5, ex(None, &newer, 12)),
        ]);
        let d = startup_decision(6, &a, None);
        assert!(
            matches!(
                &d,
                Startup::Join {
                    highest_member: 5,
                    ..
                }
            ),
            "{d:?}"
        );
    }

    #[test]
    fn removal_needs_a_quorum_of_the_voters_of_a_uniform_committed_membership() {
        // Node 3 was removed: voters {1,2,4}, a quorum is 2.
        let m = view(12, true, &[&[1, 2, 4]], &[]);
        let one = exs(&[(1, ex(None, &m, 12))]);
        assert!(removal_confirmed(3, &one).is_none());
        let two = exs(&[(1, ex(None, &m, 12)), (2, ex(None, &m, 12))]);
        assert!(removal_confirmed(3, &two).is_some());
        // Nobody answered (a partitioned node): nothing is confirmed.
        assert!(removal_confirmed(3, &BTreeMap::new()).is_none());
        // A current member never gets a confirmation.
        assert!(removal_confirmed(2, &two).is_none());
        // A voter that has not applied the removal yet does not count.
        let old = view(5, true, &[&[1, 2, 3, 4]], &[]);
        let lag = exs(&[(1, ex(None, &m, 12)), (2, ex(None, &old, 5))]);
        assert!(removal_confirmed(3, &lag).is_none());
        // Four voters need three (the rejoin bound would accept two).
        let four = view(12, true, &[&[1, 2, 4, 5]], &[]);
        let two = exs(&[(1, ex(None, &four, 12)), (2, ex(None, &four, 12))]);
        assert!(removal_confirmed(3, &two).is_none());
        let three = exs(&[
            (1, ex(None, &four, 12)),
            (2, ex(None, &four, 12)),
            (4, ex(None, &four, 12)),
        ]);
        assert!(removal_confirmed(3, &three).is_some());
        // A joint configuration waits, even without the node in either half.
        let joint = view(12, true, &[&[1, 2, 4], &[1, 2, 5]], &[]);
        let a = exs(&[(1, ex(None, &joint, 12)), (2, ex(None, &joint, 12))]);
        assert!(removal_confirmed(3, &a).is_none());
        // A removed learner: the voters' membership lacks it and its id is
        // below their highest member.
        let m = view(12, true, &[&[1, 2, 4]], &[]);
        let a = exs(&[(1, ex(None, &m, 12)), (2, ex(None, &m, 12))]);
        assert!(removal_confirmed(3, &a).is_some());
        // A demoted voter is a learner, still a member.
        let kept = view(12, true, &[&[1, 2, 4]], &[3]);
        let a = exs(&[(1, ex(None, &kept, 12)), (2, ex(None, &kept, 12))]);
        assert!(removal_confirmed(3, &a).is_none());
        // An id above every id used is not yet added, never removed.
        let a = exs(&[(1, ex(None, &m, 12)), (2, ex(None, &m, 12))]);
        assert!(removal_confirmed(9, &a).is_none());
        // A rejoining voter's answer does not count.
        let mut rj = ex(None, &m, 12);
        rj.rejoining = true;
        let a = exs(&[(1, ex(None, &m, 12)), (2, rj)]);
        assert!(removal_confirmed(3, &a).is_none());
    }

    #[test]
    fn startup_waits_on_own_leadership_or_incomparable_votes() {
        let m = view(3, true, &[&[1, 2, 3]], &[]);
        let a = exs(&[
            (2, ex(Some(Vote::new_committed(4, 1)), &m, 3)),
            (3, ex(Some(Vote::new_committed(4, 1)), &m, 3)),
        ]);
        let d = startup_decision(1, &a, None);
        assert!(matches!(d, Startup::Wait { fresh: true, .. }), "{d:?}");
    }
}
