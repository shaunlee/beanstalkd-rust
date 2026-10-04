//! The leader-side membership executor behind the admin channel
//! (docs/DESIGN.md §8, "Membership changes (P6-T4)"; docs/PLAN.md §9.3).
//!
//! [`handle`] serves an operator's change on the leader (elsewhere it
//! answers `NotLeader`): it takes the executor lock (one change at a time;
//! a second request is refused), requires the effective membership to be
//! committed and uniform, compares `expect` with its log id, checks the
//! guardrails ([`plan`], then the voters' `StatusEx` and the learner's lag),
//! and runs the change in a background task that keeps the lock until the
//! change is done. The answer is `Done` if it completes within
//! [`DONE_BOUND`], `Started` otherwise (the operator polls `Membership`).
//!
//! [`finish_joint`] finishes a joint configuration left by an earlier
//! leader towards its new half, never back to the old one.
//!
//! The guardrails the rejoin argument depends on (A1, A2) are never
//! overridable: voters only through `change_membership(BTreeSet)` from a
//! membership that lists the node as a learner, one voter change per
//! request, no voter change while a voter is rejoining or does not answer,
//! ids above every id ever used. `force` overrides only availability
//! guardrails: fewer than [`MIN_VOTERS`] voters, a lagging learner, an
//! address change over remote plaintext.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bstk_raft::wire::{AdminRequest, AdminResponse};
use bstk_raft::{NodeId, Op};
use openraft::error::{ClientWriteError, RaftError};
use openraft::{BasicNode, ChangeMembers, LogId, Membership};
use tokio::sync::OwnedMutexGuard;

use super::Core;

/// How long a request waits for its change before answering `Started`.
pub const DONE_BOUND: Duration = Duration::from_secs(2);
/// How long the voters' (and the learner's) `StatusEx` may take.
pub const PROBE_BOUND: Duration = Duration::from_secs(2);
/// A learner is promoted only if the leader's last log index is at most
/// this many entries ahead of what it replicated to the learner…
pub const PROMOTE_MAX_LAG: u64 = 100;
/// …and the learner answered the leader within `max(this, 10 ×
/// heartbeat)`.
pub const PROMOTE_MAX_SILENCE: Duration = Duration::from_secs(1);
/// Fewer voters only with `force`: a wiped voter of a two-voter membership
/// rejoins only while the other leads, one of a one-voter membership never.
pub const MIN_VOTERS: usize = 3;

pub const IN_PROGRESS: &str = "a membership change is in progress";
const FINISHING_JOINT: &str =
    "a membership change is in progress (finishing a joint configuration); retry";

/// One membership change, as decided by [`plan`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change {
    AddLearner {
        id: NodeId,
        addr: String,
    },
    /// Voters become `voters ∪ {id}`; `id` is a learner.
    Promote {
        id: NodeId,
    },
    RemoveLearner {
        id: NodeId,
    },
    /// Voters become `voters − {id}`; the node is removed entirely.
    RemoveVoter {
        id: NodeId,
    },
    SetAddr {
        id: NodeId,
        addr: String,
    },
}

impl Change {
    fn is_voter_change(&self) -> bool {
        matches!(self, Change::Promote { .. } | Change::RemoveVoter { .. })
    }
}

/// What [`plan`] needs to know besides the request.
#[derive(Debug, Clone, Copy)]
pub struct Context {
    /// The highest id that was ever a member (the applied record).
    pub highest_member: NodeId,
    pub tls: bool,
    pub plaintext_allow_remote: bool,
}

/// The checks that need nothing but the (committed, uniform) membership:
/// `Ok(None)` if there is nothing to do, `Err(reason)` if refused.
pub fn plan(
    req: &AdminRequest,
    m: &Membership<NodeId, BasicNode>,
    ctx: &Context,
) -> Result<Option<Change>, String> {
    let voters: BTreeSet<NodeId> = m.voter_ids().collect();
    let is_node = |id: NodeId| m.get_node(&id).is_some();
    match req {
        AdminRequest::Membership => Ok(None),
        AdminRequest::AddLearner { id, addr, .. } => {
            let id = *id;
            if is_node(id) {
                return match m.get_node(&id) {
                    Some(n) if n.addr == *addr && !voters.contains(&id) => Ok(None),
                    _ => Err(format!(
                        "node {id} is already a member (set-addr changes its address)"
                    )),
                };
            }
            if !(1..=bstk_raft::MAX_NODE_ID).contains(&id) {
                return Err(format!(
                    "node ids are 1..={}, not {id}",
                    bstk_raft::MAX_NODE_ID
                ));
            }
            // Ids only grow: a removed node's certificate must never be
            // readmitted, and discovery refuses ids at or below it.
            if id <= ctx.highest_member {
                return Err(format!(
                    "node id {id} is not above {}, the highest id ever used: node ids are never \
                     reused (use a new id)",
                    ctx.highest_member
                ));
            }
            check_addr(addr, ctx, false)?;
            Ok(Some(Change::AddLearner {
                id,
                addr: addr.clone(),
            }))
        }
        AdminRequest::Promote { ids, .. } => {
            let mut it = ids.iter();
            let (Some(&id), None) = (it.next(), it.next()) else {
                return Err(format!(
                    "promote one node at a time (each step a membership one voter away from the \
                     last), not {}",
                    ids.len()
                ));
            };
            if voters.contains(&id) {
                return Ok(None);
            }
            if !is_node(id) {
                return Err(format!(
                    "node {id} is not a learner: add it as a learner first"
                ));
            }
            Ok(Some(Change::Promote { id }))
        }
        AdminRequest::Remove { id, force, .. } => {
            let id = *id;
            if !is_node(id) {
                return Ok(None);
            }
            if !voters.contains(&id) {
                return Ok(Some(Change::RemoveLearner { id }));
            }
            let left = voters.len() - 1;
            if left == 0 {
                return Err(format!("node {id} is the last voter"));
            }
            if left < MIN_VOTERS && !force {
                return Err(format!(
                    "removing node {id} leaves {left} voter(s), fewer than {MIN_VOTERS}: a wiped \
                     voter of a two-voter membership rejoins only while the other leads, and the \
                     voter of a one-voter membership never (use force to do it anyway)"
                ));
            }
            Ok(Some(Change::RemoveVoter { id }))
        }
        AdminRequest::SetAddr {
            id, addr, force, ..
        } => {
            let id = *id;
            match m.get_node(&id) {
                None => Err(format!("node {id} is not a member")),
                Some(n) if n.addr == *addr => Ok(None),
                Some(_) => {
                    check_addr(addr, ctx, *force)?;
                    Ok(Some(Change::SetAddr {
                        id,
                        addr: addr.clone(),
                    }))
                }
            }
        }
    }
}

/// An address is safe to publish if a misrouted connection cannot reach the
/// wrong node unnoticed: under mTLS the dialer verifies the target's
/// certificate and the listener the hello's `to`; in plaintext only the
/// hello's `to` checks it, which a remote attacker could forge, so a remote
/// plaintext address needs `force` (an address change is where openraft
/// warns that `SetNodes` can lead to split brain).
fn check_addr(addr: &str, ctx: &Context, force: bool) -> Result<(), String> {
    if addr
        .rsplit_once(':')
        .is_none_or(|(h, p)| h.is_empty() || p.parse::<u16>().is_err())
    {
        return Err(format!("{addr:?} is not host:port"));
    }
    if ctx.tls || crate::config::is_loopback(addr) {
        return Ok(());
    }
    if !ctx.plaintext_allow_remote {
        return Err(format!(
            "{addr:?} is not a loopback address, and plaintext cluster traffic stays on loopback \
             (insecure_plaintext_allow_remote is off)"
        ));
    }
    if force {
        return Ok(());
    }
    Err(format!(
        "{addr:?} is a remote address over plaintext cluster traffic, which proves no identity: \
         use [cluster.tls] (or force)"
    ))
}

/// Who leads, as an answer to a node that does not.
fn not_leader(core: &Core) -> AdminResponse {
    let leader = core.leader().filter(|&l| l != core.id);
    let addr = leader.and_then(|l| {
        core.membership()
            .membership()
            .get_node(&l)
            .map(|n| n.addr.clone())
    });
    AdminResponse::NotLeader { leader, addr }
}

fn refused(reason: impl Into<String>) -> AdminResponse {
    AdminResponse::Refused {
        reason: reason.into(),
    }
}

fn expect_of(req: &AdminRequest) -> Option<LogId<NodeId>> {
    match req {
        AdminRequest::Membership => None,
        AdminRequest::AddLearner { expect, .. }
        | AdminRequest::Promote { expect, .. }
        | AdminRequest::Remove { expect, .. }
        | AdminRequest::SetAddr { expect, .. } => *expect,
    }
}

fn force_of(req: &AdminRequest) -> bool {
    match req {
        AdminRequest::Membership | AdminRequest::AddLearner { .. } => false,
        AdminRequest::Promote { force, .. }
        | AdminRequest::Remove { force, .. }
        | AdminRequest::SetAddr { force, .. } => *force,
    }
}

/// The effective membership and whether it is committed, read from the
/// Raft state in one step.
async fn effective(
    core: &Core,
) -> Option<(Option<LogId<NodeId>>, Membership<NodeId, BasicNode>, bool)> {
    core.raft
        .with_raft_state(|st| {
            let ms = &st.membership_state;
            let e = ms.effective();
            (
                *e.log_id(),
                e.membership().clone(),
                e.log_id() == ms.committed().log_id(),
            )
        })
        .await
        .ok()
}

/// One line per membership for the logs: voter sets and learners.
pub fn summary(m: &Membership<NodeId, BasicNode>) -> String {
    let learners: BTreeSet<NodeId> = m.learner_ids().collect();
    format!("voters {:?} learners {learners:?}", m.get_joint_config())
}

/// Serves an admin change (see the module docs).
pub async fn handle(core: Arc<Core>, req: AdminRequest, from: SocketAddr) -> AdminResponse {
    if !core.is_leader() {
        return not_leader(&core);
    }
    let Ok(guard) = core.admin_lock.clone().try_lock_owned() else {
        return refused(IN_PROGRESS);
    };
    let Some((current, m, committed)) = effective(&core).await else {
        return refused("raft has stopped");
    };
    if m.get_joint_config().len() > 1 {
        return refused(FINISHING_JOINT);
    }
    if !committed {
        return refused(format!(
            "{IN_PROGRESS} (membership {current:?} is not committed yet)"
        ));
    }
    if expect_of(&req) != current {
        return AdminResponse::Conflict { current };
    }
    // The applied record of the highest member trails the committed
    // membership by at most the apply of that entry.
    if let Some(l) = current
        && tokio::time::timeout(DONE_BOUND, core.applied_up_to(l.index))
            .await
            .is_err()
    {
        return refused(format!(
            "{IN_PROGRESS} (membership {current:?} is not applied here yet)"
        ));
    }
    let ctx = Context {
        highest_member: core.state.highest_member(),
        tls: core.tls,
        plaintext_allow_remote: core.plaintext_allow_remote,
    };
    let change = match plan(&req, &m, &ctx) {
        Ok(Some(c)) => c,
        Ok(None) => {
            tracing::info!(%from, request = ?req, membership = %summary(&m), "membership change: nothing to do");
            return AdminResponse::Done {
                log_id: current,
                note: None,
            };
        }
        Err(reason) => {
            tracing::info!(%from, request = ?req, %reason, "membership change refused");
            return refused(reason);
        }
    };
    if let Err(reason) = check_nodes(&core, &m, &change, force_of(&req)).await {
        tracing::info!(%from, request = ?req, %reason, "membership change refused");
        return refused(reason);
    }
    let note = note(&core, &m, &change);
    tracing::info!(
        %from,
        request = ?req,
        before = %summary(&m),
        "membership change started"
    );
    let (tx, rx) = tokio::sync::oneshot::channel();
    let voters: BTreeSet<NodeId> = m.voter_ids().collect();
    tokio::spawn(run(core, change, voters, from, guard, tx));
    match tokio::time::timeout(DONE_BOUND, rx).await {
        Ok(Ok(Ok(log_id))) => AdminResponse::Done {
            log_id: Some(log_id),
            note,
        },
        Ok(Ok(Err(resp))) => resp,
        Ok(Err(_)) | Err(_) => AdminResponse::Started { note },
    }
}

/// Runs `change`, holding the executor lock until it is done; sends the
/// outcome to `tx` (the request may have answered `Started` already).
async fn run(
    core: Arc<Core>,
    change: Change,
    voters: BTreeSet<NodeId>,
    from: SocketAddr,
    guard: OwnedMutexGuard<()>,
    tx: tokio::sync::oneshot::Sender<Result<LogId<NodeId>, AdminResponse>>,
) {
    #[cfg(feature = "test-hooks")]
    super::test_hooks::hold_change(&core.data_dir).await;
    let r = execute(&core, &change, voters).await;
    match &r {
        Ok(log_id) => {
            let after = effective(&core)
                .await
                .map_or_else(String::new, |(_, m, _)| summary(&m));
            tracing::info!(%from, ?change, %log_id, %after, "membership change done");
            if let Change::RemoveVoter { id } | Change::RemoveLearner { id } = change {
                drop_removed(&core, id).await;
            }
        }
        Err(resp) => tracing::warn!(%from, ?change, outcome = ?resp, "membership change failed"),
    }
    drop(guard);
    let _ = tx.send(r);
}

/// `voters`: those of the membership the change was checked against (the
/// lock keeps every other change out meanwhile).
async fn execute(
    core: &Core,
    change: &Change,
    mut voters: BTreeSet<NodeId>,
) -> Result<LogId<NodeId>, AdminResponse> {
    let r = match change {
        Change::AddLearner { id, addr } => {
            core.raft
                .add_learner(*id, BasicNode::new(addr), false)
                .await
        }
        // A1: a voter is only ever made through `ReplaceAllVoters` (what a
        // `BTreeSet` converts to), which openraft refuses for a node the
        // membership does not list yet; never `AddVoters` / `AddVoterIds`.
        Change::Promote { id } => {
            voters.insert(*id);
            core.raft.change_membership(voters, false).await
        }
        Change::RemoveVoter { id } => {
            voters.remove(id);
            // `retain = false`: the node leaves the membership entirely.
            core.raft.change_membership(voters, false).await
        }
        Change::RemoveLearner { id } => {
            core.raft
                .change_membership(ChangeMembers::RemoveNodes([*id].into()), false)
                .await
        }
        Change::SetAddr { id, addr } => {
            let nodes = BTreeMap::from([(*id, BasicNode::new(addr))]);
            core.raft
                .change_membership(ChangeMembers::SetNodes(nodes), false)
                .await
        }
    };
    r.map(|resp| resp.log_id).map_err(raft_error)
}

fn raft_error(e: RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>) -> AdminResponse {
    match e {
        RaftError::APIError(ClientWriteError::ForwardToLeader(f)) => AdminResponse::NotLeader {
            leader: f.leader_id,
            addr: f.leader_node.map(|n| n.addr),
        },
        RaftError::APIError(ClientWriteError::ChangeMembershipError(e)) => refused(e.to_string()),
        RaftError::Fatal(e) => refused(format!("raft has stopped: {e}")),
    }
}

/// Closes out a removed node's connections at once (the leader's duties
/// would within 100 ms, and cover a change that completes after a leader
/// change). A leader that removed itself has stepped down: nothing to do.
async fn drop_removed(core: &Core, id: NodeId) {
    if !core.is_leader() || id == core.id || !core.owns_connections(id) {
        return;
    }
    let op = Op::DropNode {
        node: id,
        up_to_local: core.state.highest_local(id),
    };
    if core.propose(op).await.is_ok() {
        tracing::info!(node = id, "removed node's connections: proposed DropNode");
    }
}

/// The checks that need the other nodes (see the module docs): for a voter
/// change, every voter but this leader must answer `StatusEx` within
/// [`PROBE_BOUND`] and not report `rejoining` (A2), except the voter being
/// removed, which may be gone (removing a dead voter is the common case)
/// but must not be rejoining; a learner to promote must answer, not be
/// rejoining and, unless `force`, be caught up.
async fn check_nodes(
    core: &Core,
    m: &Membership<NodeId, BasicNode>,
    change: &Change,
    force: bool,
) -> Result<(), String> {
    if !change.is_voter_change() {
        return Ok(());
    }
    // First, as it needs no network: a learner that never ran would
    // otherwise be reported as not answering.
    if let Change::Promote { id } = change
        && !force
    {
        caught_up(core, *id)?;
    }
    let mut ask: BTreeSet<NodeId> = m.voter_ids().filter(|&v| v != core.id).collect();
    let (optional, learner) = match change {
        Change::RemoveVoter { id } => (Some(*id), None),
        Change::Promote { id } => {
            ask.insert(*id);
            (None, Some(*id))
        }
        _ => (None, None),
    };
    let answers = futures::future::join_all(ask.iter().map(|&n| async move {
        let r = tokio::time::timeout(PROBE_BOUND, core.net.status_ex(n)).await;
        (n, r)
    }))
    .await;
    for (n, r) in answers {
        let what = if Some(n) == learner {
            "learner"
        } else {
            "voter"
        };
        match r {
            Ok(Ok(s)) if s.rejoining => {
                return Err(if Some(n) == optional {
                    format!(
                        "node {n} is rejoining: stop it before removing it (a stopped node need \
                         not answer)"
                    )
                } else {
                    format!(
                        "{what} {n} is rejoining: voter changes wait until it has caught up \
                         (the rejoin argument assumes it, docs/DESIGN.md §8 A2)"
                    )
                });
            }
            Ok(Ok(_)) => {}
            _ if Some(n) == optional => {}
            Ok(Err(e)) => {
                return Err(format!(
                    "{what} {n} did not answer its status ({e}): voter changes need every voter's \
                     answer (one rejoining unseen must not be overlooked)"
                ));
            }
            Err(_) => {
                return Err(format!(
                    "{what} {n} did not answer its status within {PROBE_BOUND:?}: voter changes \
                     need every voter's answer"
                ));
            }
        }
    }
    Ok(())
}

/// Whether learner `id` is close enough behind the leader to be promoted
/// without stalling commits (see [`PROMOTE_MAX_LAG`]).
fn caught_up(core: &Core, id: NodeId) -> Result<(), String> {
    let (matched, last) = {
        let m = core.metrics.borrow();
        let matched = m
            .replication
            .as_ref()
            .and_then(|r| r.get(&id).copied().flatten());
        (matched.map(|l| l.index), m.last_log_index)
    };
    let silent_for = core.net.last_response(id).map(|t| t.elapsed());
    lag_verdict(id, matched, last, silent_for, lag_bound(core.heartbeat))
}

/// [`caught_up`] on the leader's replication state: `matched`, the last
/// index replicated to the learner; `last`, the leader's last log index;
/// `silent_for`, since the learner last answered the leader.
fn lag_verdict(
    id: NodeId,
    matched: Option<u64>,
    last: Option<u64>,
    silent_for: Option<Duration>,
    bound: Duration,
) -> Result<(), String> {
    let Some(matched) = matched else {
        return Err(format!(
            "learner {id} has not replicated anything yet: start it and let it catch up (or force)"
        ));
    };
    let lag = last.unwrap_or(0).saturating_sub(matched);
    let silent = silent_for.is_none_or(|d| d > bound);
    if lag > PROMOTE_MAX_LAG || silent {
        return Err(format!(
            "learner {id} is not caught up ({lag} entries behind, last answer {}): wait or force",
            if silent {
                format!("more than {bound:?} ago")
            } else {
                "recent".into()
            }
        ));
    }
    Ok(())
}

fn lag_bound(heartbeat: Duration) -> Duration {
    PROMOTE_MAX_SILENCE.max(heartbeat.saturating_mul(10))
}

/// What the operator must know about a change (`Started` / `Done` note).
fn note(core: &Core, m: &Membership<NodeId, BasicNode>, change: &Change) -> Option<String> {
    let voters = m.voter_ids().count();
    let mut notes = Vec::new();
    let after = match change {
        Change::Promote { .. } => voters + 1,
        Change::RemoveVoter { .. } => voters - 1,
        _ => voters,
    };
    if let Change::RemoveVoter { id } | Change::RemoveLearner { id } = change {
        notes.push(if *id == core.id {
            format!(
                "node {id} (the leader that ran this change) steps down once it is committed; \
                 another node leads, and node {id} is not told it was removed: stop its process"
            )
        } else {
            format!(
                "node {id} is not told it was removed (it isolates itself): stop its process; its \
                 id can never be used again"
            )
        });
    }
    if change.is_voter_change() && after % 2 == 0 {
        notes.push(format!(
            "{after} voters: an even count tolerates no more failures than {} voters; change to \
             an odd count",
            after - 1
        ));
    }
    if change.is_voter_change() && after < MIN_VOTERS {
        notes.push(format!(
            "{after} voter(s): below {MIN_VOTERS}, a wiped voter may be unable to rejoin"
        ));
    }
    (!notes.is_empty()).then(|| notes.join("; "))
}

/// Leader-side: a joint configuration `(C, C')` in the effective membership
/// (left by a leader that lost leadership between openraft's two steps) is
/// finished towards `C'` once it is committed, never back to `C`; requests
/// are refused meanwhile. Shares the executor lock, so a change in flight
/// (whose own joint step looks the same) is never "finished" by this task.
/// Runs until Raft stops.
pub async fn finish_joint(core: Arc<Core>) {
    let mut view = core.watch_view();
    loop {
        if core.is_leader() && core.membership().membership().get_joint_config().len() > 1 {
            let guard = core.admin_lock.clone().lock_owned().await;
            if !finish_once(&core).await {
                drop(guard);
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
            drop(guard);
        }
        tokio::select! {
            r = view.changed() => if r.is_err() { return },
            () = tokio::time::sleep(Duration::from_millis(500)) => {}
        }
    }
}

/// One attempt; `false` to try again shortly.
async fn finish_once(core: &Core) -> bool {
    if !core.is_leader() {
        return true;
    }
    let Some((log_id, m, committed)) = effective(core).await else {
        return true;
    };
    let configs = m.get_joint_config();
    let Some(goal) = joint_goal(configs) else {
        return true;
    };
    // openraft refuses a change (`InProgress`) until the joint entry is
    // committed; this leader commits it with its first entry.
    if !committed {
        return false;
    }
    let before = summary(&m);
    match core.raft.change_membership(goal.clone(), false).await {
        Ok(r) => {
            tracing::warn!(
                ?log_id,
                %before,
                after = ?goal,
                log_id = %r.log_id,
                "finished a joint configuration left by an earlier leader"
            );
            true
        }
        Err(e) => {
            tracing::warn!(%before, "finishing a joint configuration: {e}");
            false
        }
    }
}

/// The voters a joint configuration is finished towards: its new (last)
/// half. `None` for a uniform one.
pub fn joint_goal(configs: &[BTreeSet<NodeId>]) -> Option<BTreeSet<NodeId>> {
    match configs {
        [_, .., last] => Some(last.clone()),
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn set(ids: &[NodeId]) -> BTreeSet<NodeId> {
        ids.iter().copied().collect()
    }

    /// Voters `voters`, plus learners `learners`; node `i` at `127.0.0.1:i`.
    fn mem(voters: &[NodeId], learners: &[NodeId]) -> Membership<NodeId, BasicNode> {
        let nodes: BTreeMap<NodeId, BasicNode> = voters
            .iter()
            .chain(learners)
            .map(|&i| (i, BasicNode::new(format!("127.0.0.1:{i}"))))
            .collect();
        Membership::new(vec![set(voters)], nodes)
    }

    const CTX: Context = Context {
        highest_member: 4,
        tls: false,
        plaintext_allow_remote: false,
    };

    fn add(id: NodeId, addr: &str) -> AdminRequest {
        AdminRequest::AddLearner {
            id,
            addr: addr.into(),
            expect: None,
        }
    }

    fn promote(ids: &[NodeId], force: bool) -> AdminRequest {
        AdminRequest::Promote {
            ids: set(ids),
            expect: None,
            force,
        }
    }

    fn remove(id: NodeId, force: bool) -> AdminRequest {
        AdminRequest::Remove {
            id,
            expect: None,
            force,
        }
    }

    fn set_addr(id: NodeId, addr: &str, force: bool) -> AdminRequest {
        AdminRequest::SetAddr {
            id,
            addr: addr.into(),
            expect: None,
            force,
        }
    }

    #[test]
    fn ids_only_grow() {
        let m = mem(&[1, 2, 3], &[]);
        // 4 was a member once (removed): never again; nor anything below.
        for id in [4, 3, 1] {
            let e = plan(&add(id, "127.0.0.1:9"), &m, &CTX).unwrap_err();
            assert!(
                e.contains("never reused") || e.contains("already a member"),
                "{id}: {e}"
            );
        }
        assert_eq!(
            plan(&add(5, "127.0.0.1:9"), &m, &CTX),
            Ok(Some(Change::AddLearner {
                id: 5,
                addr: "127.0.0.1:9".into()
            }))
        );
        assert!(plan(&add(0, "127.0.0.1:9"), &m, &CTX).is_err());
        assert!(plan(&add(bstk_raft::MAX_NODE_ID + 1, "127.0.0.1:9"), &m, &CTX).is_err());
        // Adding an existing learner again at its address: nothing to do.
        let m = mem(&[1, 2, 3], &[4]);
        assert_eq!(plan(&add(4, "127.0.0.1:4"), &m, &CTX), Ok(None));
        assert!(plan(&add(4, "127.0.0.1:5"), &m, &CTX).is_err());
    }

    #[test]
    fn promote_one_learner_at_a_time() {
        let m = mem(&[1, 2, 3], &[4, 5]);
        assert_eq!(
            plan(&promote(&[4], false), &m, &CTX),
            Ok(Some(Change::Promote { id: 4 }))
        );
        let e = plan(&promote(&[4, 5], true), &m, &CTX).unwrap_err();
        assert!(e.contains("one node at a time"), "{e}");
        assert!(plan(&promote(&[], false), &m, &CTX).is_err());
        // Not a learner: never made a voter directly (A1).
        let e = plan(&promote(&[6], true), &m, &CTX).unwrap_err();
        assert!(e.contains("not a learner"), "{e}");
        assert_eq!(plan(&promote(&[2], false), &m, &CTX), Ok(None));
    }

    #[test]
    fn remove_keeps_three_voters_unless_forced() {
        let m = mem(&[1, 2, 3], &[4]);
        assert_eq!(
            plan(&remove(4, false), &m, &CTX),
            Ok(Some(Change::RemoveLearner { id: 4 }))
        );
        let e = plan(&remove(2, false), &m, &CTX).unwrap_err();
        assert!(e.contains("fewer than 3") && e.contains("force"), "{e}");
        assert_eq!(
            plan(&remove(2, true), &m, &CTX),
            Ok(Some(Change::RemoveVoter { id: 2 }))
        );
        assert_eq!(plan(&remove(9, false), &m, &CTX), Ok(None));
        let m5 = mem(&[1, 2, 3, 4, 5], &[]);
        assert_eq!(
            plan(&remove(5, false), &m5, &CTX),
            Ok(Some(Change::RemoveVoter { id: 5 }))
        );
        let one = mem(&[1], &[]);
        assert!(plan(&remove(1, true), &one, &CTX).is_err());
    }

    #[test]
    fn addresses_stay_on_loopback_or_need_tls() {
        let m = mem(&[1, 2, 3], &[]);
        assert_eq!(
            plan(&set_addr(2, "127.0.0.1:77", false), &m, &CTX),
            Ok(Some(Change::SetAddr {
                id: 2,
                addr: "127.0.0.1:77".into()
            }))
        );
        assert_eq!(plan(&set_addr(2, "127.0.0.1:2", false), &m, &CTX), Ok(None));
        assert!(plan(&set_addr(9, "127.0.0.1:77", false), &m, &CTX).is_err());
        assert!(plan(&set_addr(2, "no-port", false), &m, &CTX).is_err());
        // Plaintext, loopback only: refused even with force.
        let e = plan(&set_addr(2, "10.0.0.2:1", true), &m, &CTX).unwrap_err();
        assert!(e.contains("loopback"), "{e}");
        assert!(plan(&add(5, "10.0.0.5:1"), &m, &CTX).is_err());
        // Plaintext allowed off loopback: only with force.
        let remote = Context {
            plaintext_allow_remote: true,
            ..CTX
        };
        let e = plan(&set_addr(2, "10.0.0.2:1", false), &m, &remote).unwrap_err();
        assert!(e.contains("force"), "{e}");
        assert!(plan(&set_addr(2, "10.0.0.2:1", true), &m, &remote).is_ok());
        // mTLS: any address.
        let tls = Context { tls: true, ..CTX };
        assert!(plan(&set_addr(2, "node2.example:11400", false), &m, &tls).is_ok());
        assert!(plan(&add(5, "10.0.0.5:1"), &m, &tls).is_ok());
    }

    /// A leftover joint configuration is finished towards its new half,
    /// never back to the old one.
    #[test]
    fn joint_is_finished_towards_the_new_half() {
        assert_eq!(
            joint_goal(&[set(&[1, 2, 3]), set(&[1, 2, 3, 4])]),
            Some(set(&[1, 2, 3, 4]))
        );
        assert_eq!(
            joint_goal(&[set(&[1, 2, 3]), set(&[1, 2])]),
            Some(set(&[1, 2]))
        );
        assert_eq!(joint_goal(&[set(&[1, 2, 3])]), None);
        assert_eq!(joint_goal(&[]), None);
    }

    #[test]
    fn only_a_caught_up_learner_is_promoted() {
        let b = Duration::from_secs(1);
        let recent = Some(Duration::from_millis(100));
        assert!(lag_verdict(4, Some(900), Some(1000), recent, b).is_ok());
        assert!(lag_verdict(4, Some(1000), Some(1000), recent, b).is_ok());
        let e = lag_verdict(4, Some(899), Some(1000), recent, b).unwrap_err();
        assert!(e.contains("101 entries behind"), "{e}");
        let e = lag_verdict(4, None, Some(1000), recent, b).unwrap_err();
        assert!(e.contains("not replicated anything"), "{e}");
        // Matched, but silent since: perhaps gone.
        let e = lag_verdict(4, Some(1000), Some(1000), Some(2 * b), b).unwrap_err();
        assert!(e.contains("more than"), "{e}");
        assert!(lag_verdict(4, Some(1000), Some(1000), None, b).is_err());
    }

    #[test]
    fn promote_lag_bound_scales_with_the_heartbeat() {
        assert_eq!(
            lag_bound(Duration::from_millis(100)),
            Duration::from_secs(1)
        );
        assert_eq!(
            lag_bound(Duration::from_millis(500)),
            Duration::from_secs(5)
        );
    }
}
