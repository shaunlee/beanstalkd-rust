//! Membership changes in the in-process harness (P6-T7).
//!
//! The changes go through the server's own executor,
//! `bstk_raft::admin::handle`, on whichever node leads ([`NodeInc`] is its
//! host), so every guardrail is exercised as in production: the executor
//! lock, compare-and-set on the membership log id, ids above every id ever
//! used, voters only from learners, one voter per step, no voter change
//! while a voter is rejoining or silent, the 3-voter floor, only a caught-up
//! learner promoted; `finish_joint` runs on every node. The operator here
//! behaves like the `beanstalkd-rs cluster` command: it reads the leader's
//! membership log id as `expect`, follows `NotLeader`, and gives up on
//! `Conflict` and `Refused` (a multi-step flow such as a replace waits and
//! retries, as an operator following the runbook would).
//!
//! Faults ([`MFault`]): add a learner (a new id; the node started before
//! or after the add), promote a learner, remove the leader, a node whose
//! clients hold reservations, a learner or a voter, replace a voter with a
//! new id (add, start, catch up, promote, remove the old one), replace a
//! node by the same id with a wiped disk at a new address (rejoin, then
//! set-addr), change an address, two concurrent changes, a voter change
//! whose leader crashes as soon as its joint configuration appears (between
//! the two steps), and restarting a removed node (with its data or wiped).
//! A removed node is stopped (the runbook's "stop its process") 0–2 s after
//! its removal is committed. Changes also happen during partitions, since
//! the schedule mixes them with the network faults.
//!
//! Invariants (in addition to the harness's):
//!
//! 1. after heal and settle, the leader's membership is committed and
//!    uniform and every member holds it; walking the committed log, every
//!    membership step is explained by one requested change that was not
//!    refused (guardrail refusals and conflicts must leave no trace), every
//!    `Done` answer names a committed membership with the requested effect,
//!    a joint `(C, C')` is followed by `C'` (never back to `C`, A1), and a
//!    voter step changes one voter;
//! 2. every connection owner in every member's state machine is a member,
//!    and no running node owns a connection of an earlier incarnation (no
//!    leader drops a live node's, so it would hold its reservations);
//! 3. a removed node's connections (and so its reservations) are gone
//!    within [`RELEASE_BOUND`] of the later of its removal and the last
//!    fault before (so a partition that keeps the cluster from committing
//!    does not count);
//! 4. no committed entry diverges across nodes (the ledger, as before);
//! 5. the highest member id never decreases along the log and is the same
//!    on every node at an index, and no id is ever added again or added at
//!    or below an id used before;
//! 6. no node adopts a vote from a rejoining voter: a rejoin's adopted vote
//!    is at most the highest vote of the answers from voters holding the
//!    membership and not rejoining by the harness's own record (checked in
//!    `discover`), a removed node's discovery never decides join or rejoin,
//!    and no status answer hides a rejoin.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bstk_raft::admin::AdminHost;
use bstk_raft::forward::ForwardError;
use bstk_raft::sim::SimRng;
use bstk_raft::status::{NodeStatusEx, StatusTransport};
use bstk_raft::wire::{AdminRequest, AdminResponse};
use bstk_raft::{NodeId, Op, TypeConfig};
use openraft::{BasicNode, Entry, EntryPayload, LogId, Membership, Raft, StoredMembership};
use tokio::time::Instant;

use super::{NodeInc, RunShared, StartMode, lock, wait_until};

/// The server's default-sized `node_timeout` in the multi-process harness;
/// the in-process leader drops non-member owners at once, so the bound
/// mostly covers an election after removing the leader.
const NODE_TIMEOUT: Duration = Duration::from_secs(1);
/// Invariant 3: `2 × node_timeout` plus a margin.
pub const RELEASE_BOUND: Duration = Duration::from_secs(5);
const _: () = assert!(RELEASE_BOUND.as_secs() >= 2 * NODE_TIMEOUT.as_secs());
/// How long a multi-step flow keeps retrying one step.
const STEP_BUDGET: Duration = Duration::from_secs(20);

#[derive(Debug, Clone, PartialEq)]
pub enum MFault {
    Add { start_first: bool },
    Promote(u64),
    Remove(Target),
    Replace(u64),
    ReplaceFailed(u64),
    ReplaceWiped(u64),
    SetAddr(u64),
    Concurrent(u64),
    LeaderLoss(u64),
    RestartRemoved { wipe: bool, draw: u64 },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Target {
    Leader,
    /// The node whose clients hold the most reservations.
    Holding,
    Learner(u64),
    Voter(u64),
}

pub(super) fn generate(r: &mut SimRng) -> MFault {
    match r.range(0, 17) {
        0..=2 => MFault::Add {
            start_first: r.range(0, 1) == 0,
        },
        3 | 4 => MFault::Promote(r.next_u64()),
        5 => MFault::Remove(Target::Leader),
        6 => MFault::Remove(Target::Holding),
        7 => MFault::Remove(Target::Learner(r.next_u64())),
        8 => MFault::Remove(Target::Voter(r.next_u64())),
        9 => MFault::Replace(r.next_u64()),
        10 => MFault::ReplaceFailed(r.next_u64()),
        11 => MFault::ReplaceWiped(r.next_u64()),
        12 => MFault::SetAddr(r.next_u64()),
        13 => MFault::Concurrent(r.next_u64()),
        14 | 15 => MFault::LeaderLoss(r.next_u64()),
        _ => MFault::RestartRemoved {
            wipe: r.range(0, 1) == 0,
            draw: r.next_u64(),
        },
    }
}

/// A node's membership address (`host:port`, as the executor requires;
/// the simulated network routes by id, as mTLS binds an id to a
/// certificate). `generation` distinguishes the addresses of a moved node.
pub fn addr_of(id: NodeId, generation: u64) -> String {
    format!("node{id}-{generation}.sim:11400")
}

/// The answer an operator got, or why there was none.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Outcome {
    Done(Option<LogId<NodeId>>),
    Started,
    NotLeader,
    Conflict,
    Refused(String),
    /// The request was cut short (its leader crashed).
    Lost,
    NoLeader,
}

impl Outcome {
    /// Whether the change may be in the log: everything but an answer that
    /// says it was refused before anything was appended.
    fn may_have_run(&self) -> bool {
        match self {
            Outcome::Conflict | Outcome::NoLeader => false,
            // openraft's own refusals (`InProgress`, `LearnerNotFound`, …)
            // come before the append; a stopped Raft may not.
            Outcome::Refused(r) => r.starts_with("raft has stopped"),
            _ => true,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Attempt {
    pub req: AdminRequest,
    pub outcome: Outcome,
}

impl AdminHost for NodeInc {
    fn id(&self) -> NodeId {
        self.id
    }

    fn raft(&self) -> &Raft<TypeConfig> {
        &self.raft
    }

    fn is_leader(&self) -> bool {
        NodeInc::is_leader(self)
    }

    fn leader(&self) -> Option<NodeId> {
        self.raft.metrics().borrow().current_leader
    }

    fn membership(&self) -> Arc<StoredMembership<NodeId, BasicNode>> {
        self.effective()
    }

    fn admin_lock(&self) -> &Arc<tokio::sync::Mutex<()>> {
        &self.admin_lock
    }

    fn highest_member(&self) -> NodeId {
        self.state.highest_member()
    }

    fn tls(&self) -> bool {
        true
    }

    fn plaintext_allow_remote(&self) -> bool {
        false
    }

    fn heartbeat(&self) -> Duration {
        Duration::from_millis(self.run.raft_cfg.heartbeat_interval)
    }

    async fn applied_up_to(&self, index: u64) {
        let mut rx = self.state.subscribe();
        while self.state.last_applied().is_none_or(|l| l.index < index) {
            if rx.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }

    fn status_ex(
        &self,
        node: NodeId,
    ) -> impl Future<Output = Result<NodeStatusEx, ForwardError>> + Send {
        let net = self.run.net.node(self.id);
        async move { net.status_ex(node).await }
    }

    fn silent_for(&self, node: NodeId) -> Option<Duration> {
        self.run
            .net
            .last_response(self.id, node)
            .map(|t| t.elapsed())
    }

    fn owns_connections(&self, node: NodeId) -> bool {
        self.state
            .conn_ids()
            .iter()
            .any(|&c| bstk_raft::owner_of(c) == node)
    }

    fn highest_local(&self, node: NodeId) -> u64 {
        self.state.highest_local(node)
    }

    async fn propose(&self, op: Op) -> bool {
        NodeInc::propose(self, op).await
    }

    fn spawn(&self, task: impl Future<Output = ()> + Send + 'static) {
        self.track(tokio::spawn(task).abort_handle());
    }
}

fn operator_addr() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 9))
}

fn set_expect(req: &mut AdminRequest, current: Option<LogId<NodeId>>) {
    match req {
        AdminRequest::Membership => {}
        AdminRequest::AddLearner { expect, .. }
        | AdminRequest::Promote { expect, .. }
        | AdminRequest::Remove { expect, .. }
        | AdminRequest::SetAddr { expect, .. } => *expect = current,
    }
}

async fn wait_leader(run: &RunShared, within: Duration) -> Option<Arc<NodeInc>> {
    let deadline = Instant::now() + within;
    loop {
        if let Some(l) = run.leader() {
            return Some(l);
        }
        if Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// One operator request, as the CLI sends it: `expect` read from the
/// leader, `NotLeader` followed (at most 8 times). Every request sent is
/// recorded with its answer.
pub(super) async fn request(run: &Arc<RunShared>, req: AdminRequest) -> Outcome {
    if run.partitioned.load(Ordering::Relaxed) {
        run.count("ChangeDuringPartition");
    }
    let mut out = Outcome::NoLeader;
    for _ in 0..8 {
        let Some(leader) = wait_leader(run, Duration::from_secs(5)).await else {
            break;
        };
        let Some((current, _, _)) = bstk_raft::admin::effective(&leader.raft).await else {
            continue;
        };
        let mut r = req.clone();
        set_expect(&mut r, current);
        let host = leader.clone();
        let sent = r.clone();
        let task =
            tokio::spawn(async move { bstk_raft::admin::handle(host, r, operator_addr()).await });
        leader.track(task.abort_handle());
        let from = leader.id;
        drop(leader);
        out = match task.await {
            Ok(AdminResponse::Done { log_id, .. }) => Outcome::Done(log_id),
            Ok(AdminResponse::Started { .. }) => Outcome::Started,
            Ok(AdminResponse::NotLeader { .. }) => Outcome::NotLeader,
            Ok(AdminResponse::Conflict { .. }) => Outcome::Conflict,
            Ok(AdminResponse::Refused { reason }) => Outcome::Refused(reason),
            Ok(other) => {
                run.problem(format!(
                    "admin request {sent:?}: unexpected answer {other:?}"
                ));
                Outcome::Refused(format!("{other:?}"))
            }
            Err(_) => Outcome::Lost,
        };
        run.event(format!("operator: {sent:?} on leader {from} -> {out:?}"));
        lock(&run.attempts).push(Attempt {
            req: sent,
            outcome: out.clone(),
        });
        if out != Outcome::NotLeader {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    out
}

/// The leader's effective membership if it is committed and uniform.
async fn settled(
    run: &RunShared,
) -> Option<(Option<LogId<NodeId>>, Membership<NodeId, BasicNode>)> {
    let l = run.leader()?;
    let (log_id, m, committed) = bstk_raft::admin::effective(&l.raft).await?;
    (committed && m.get_joint_config().len() == 1).then_some((log_id, m))
}

/// [`settled`], the same twice 500 ms apart from a leader a quorum
/// acknowledged, with no change running in its executor: what the final
/// checks take as the cluster's membership.
async fn stable(run: &RunShared) -> Option<(Option<LogId<NodeId>>, Membership<NodeId, BasicNode>)> {
    let first = settled(run).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let l = run.leader()?;
    let m = l.raft.metrics().borrow().clone();
    if m.millis_since_quorum_ack.is_none_or(|ms| ms > 1000)
        && m.membership_config.membership().voter_ids().count() > 1
    {
        return None;
    }
    let idle = l.admin_lock.try_lock().is_ok();
    let second = settled(run).await?;
    (idle && first.0 == second.0).then_some(second)
}

/// Whether `m` shows the effect `req` asks for.
fn has_effect(req: &AdminRequest, m: &Membership<NodeId, BasicNode>) -> bool {
    match req {
        AdminRequest::Membership => true,
        AdminRequest::AddLearner { id, addr, .. } => {
            m.get_node(id).is_some_and(|n| n.addr == *addr)
        }
        AdminRequest::Promote { ids, .. } => {
            let voters: BTreeSet<NodeId> = m.voter_ids().collect();
            ids.iter().all(|i| voters.contains(i))
        }
        AdminRequest::Remove { id, .. } => m.get_node(id).is_none(),
        AdminRequest::SetAddr { id, addr, .. } => m.get_node(id).is_some_and(|n| n.addr == *addr),
    }
}

/// A step of a multi-step flow: retries until the leader's committed
/// membership shows the effect, at most `budget`.
async fn step(run: &Arc<RunShared>, req: AdminRequest, budget: Duration) -> bool {
    let deadline = Instant::now() + budget;
    loop {
        if let Some((_, m)) = settled(run).await
            && has_effect(&req, &m)
        {
            return true;
        }
        if Instant::now() >= deadline {
            run.count("FlowStepGaveUp");
            return false;
        }
        let out = request(run, req.clone()).await;
        if matches!(out, Outcome::Started | Outcome::Lost | Outcome::NotLeader) {
            // Give the change time to finish (or a new leader to finish it).
            let _ = wait_until(Duration::from_secs(3), || false).await;
        } else {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}

/// The next unused spare id, if any is left.
fn alloc_spare(run: &RunShared) -> Option<NodeId> {
    let id = run.next_spare.fetch_add(1, Ordering::Relaxed);
    if run.all_ids.contains(&id) {
        Some(id)
    } else {
        run.count("SparesExhausted");
        None
    }
}

fn pick<T: Copy>(v: &[T], draw: u64) -> Option<T> {
    (!v.is_empty()).then(|| v[(draw % v.len() as u64) as usize])
}

/// Stops removed node `id` (the runbook's "stop its process") `delay`
/// after its removal is committed, if it is within 10 s.
fn retire_later(run: &Arc<RunShared>, id: NodeId, delay: Duration) {
    let run2 = run.clone();
    let h = tokio::spawn(async move {
        let removed = wait_until(Duration::from_secs(10), || {
            lock(&run2.ledger).removed.contains_key(&id)
        })
        .await;
        if !removed {
            return;
        }
        tokio::time::sleep(delay).await;
        retire(&run2, id);
    });
    lock(&run.operators).push(h);
}

fn retire(run: &RunShared, id: NodeId) {
    if lock(&run.retired).insert(id) {
        run.event(format!("operator stops removed node {id}"));
    }
    run.crash(id);
}

/// Starts a new node of id `id`.
async fn start_new(run: &Arc<RunShared>, id: NodeId) {
    if let Err(e) = run.start(id, StartMode::Normal).await {
        run.problem(e);
    }
}

async fn add(run: &Arc<RunShared>, start_first: bool) -> Option<NodeId> {
    let id = alloc_spare(run)?;
    if start_first {
        start_new(run, id).await;
    }
    let out = request(
        run,
        AdminRequest::AddLearner {
            id,
            addr: addr_of(id, 0),
            expect: None,
        },
    )
    .await;
    if !start_first && out.may_have_run() {
        start_new(run, id).await;
    }
    Some(id)
}

fn removal(id: NodeId) -> AdminRequest {
    AdminRequest::Remove {
        id,
        expect: None,
        force: false,
    }
}

fn promotion(id: NodeId) -> AdminRequest {
    AdminRequest::Promote {
        ids: [id].into(),
        expect: None,
        force: false,
    }
}

/// Removes `id`, then stops it once removed. `what` names the target for
/// the run's statistics.
async fn remove(run: &Arc<RunShared>, id: NodeId, draw: u64, what: &str) {
    let out = request(run, removal(id)).await;
    if matches!(out, Outcome::Done(_)) {
        run.count(&format!("Removed{what}"));
    }
    if out.may_have_run() {
        retire_later(run, id, Duration::from_millis(draw % 2000));
    }
}

/// Learners and voters of the leader's settled membership.
async fn roles(run: &RunShared) -> Option<(Vec<NodeId>, Vec<NodeId>)> {
    let (_, m) = settled(run).await?;
    Some((m.learner_ids().collect(), m.voter_ids().collect()))
}

/// The wipe rule: a wipe must leave, in every voter set of the current
/// membership (the latest committed one, and the leader's effective one if
/// a change is in flight), at most `n - quorum(n)` voters without their
/// data (wiped or rejoining); none while a joint configuration is current.
pub(super) fn wipe_allowed(run: &RunShared, id: NodeId) -> Result<(), String> {
    let mut sets: Vec<Vec<BTreeSet<NodeId>>> = Vec::new();
    if let Some((_, c)) = &lock(&run.ledger).latest {
        sets.push(c.clone());
    }
    if let Some(l) = run.leader() {
        sets.push(l.effective().membership().get_joint_config().clone());
    }
    if sets.is_empty() {
        sets.push(vec![run.initial.iter().copied().collect()]);
    }
    let rejoining = run.rejoining.now();
    for configs in sets {
        if configs.len() > 1 {
            return Err("a joint configuration is current".into());
        }
        for c in configs {
            let k = c
                .iter()
                .filter(|&&x| x != id && rejoining.contains(&x))
                .count()
                + usize::from(c.contains(&id));
            let spare = c.len() - bstk_raft::status::quorum(c.len());
            if k > spare {
                return Err(format!("voters {c:?} cannot spare it: others rejoining"));
            }
        }
    }
    Ok(())
}

/// Runs a membership fault (in an operator task of its own).
pub(super) async fn apply(run: &Arc<RunShared>, f: &MFault) {
    run.event(format!("membership fault {f:?}"));
    match f.clone() {
        MFault::Add { start_first } => {
            add(run, start_first).await;
        }
        MFault::Promote(draw) => {
            let Some((learners, _)) = roles(run).await else {
                return run.count("PromoteSkipped");
            };
            match pick(&learners, draw) {
                Some(x) => {
                    request(run, promotion(x)).await;
                }
                None => run.count("PromoteSkipped"),
            }
        }
        MFault::Remove(t) => {
            let Some((learners, voters)) = roles(run).await else {
                return run.count("RemoveSkipped");
            };
            let x = match t {
                Target::Leader => run.leader().map(|l| l.id),
                Target::Holding => {
                    let members: BTreeSet<NodeId> =
                        learners.iter().chain(&voters).copied().collect();
                    let mut held: BTreeMap<NodeId, usize> = BTreeMap::new();
                    for (n, c) in lock(&run.holding).values() {
                        *held.entry(*n).or_default() += c;
                    }
                    held.into_iter()
                        .filter(|(n, c)| *c > 0 && members.contains(n))
                        .max_by_key(|(_, c)| *c)
                        .map(|(n, _)| n)
                }
                Target::Learner(d) => pick(&learners, d),
                Target::Voter(d) => pick(&voters, d),
            };
            let what = match t {
                Target::Leader => "Leader",
                Target::Holding => "NodeHoldingReservations",
                Target::Learner(_) => "Learner",
                Target::Voter(_) => "Voter",
            };
            match x {
                Some(x) => remove(run, x, run.elapsed().as_millis() as u64, what).await,
                None => run.count("RemoveSkipped"),
            }
        }
        MFault::Replace(draw) => replace(run, draw, false).await,
        MFault::ReplaceFailed(draw) => replace(run, draw, true).await,
        MFault::ReplaceWiped(draw) => replace_wiped(run, draw).await,
        MFault::SetAddr(draw) => {
            let Some((learners, voters)) = roles(run).await else {
                return;
            };
            let all: Vec<NodeId> = learners.into_iter().chain(voters).collect();
            if let Some(x) = pick(&all, draw) {
                let req = AdminRequest::SetAddr {
                    id: x,
                    addr: addr_of(x, run.elapsed().as_millis() as u64),
                    expect: None,
                    force: false,
                };
                request(run, req).await;
            }
        }
        MFault::Concurrent(draw) => concurrent(run, draw).await,
        MFault::LeaderLoss(draw) => leader_loss(run, draw).await,
        MFault::RestartRemoved { wipe, draw } => {
            let retired: Vec<NodeId> = lock(&run.retired).iter().copied().collect();
            let Some(x) = pick(&retired, draw) else {
                return run.count("RestartRemovedSkipped");
            };
            if run.running().contains(&x) {
                return;
            }
            lock(&run.refused).remove(&x);
            let mode = if wipe {
                StartMode::Wipe
            } else {
                StartMode::Normal
            };
            run.event(format!("restart removed node {x} (wiped: {wipe})"));
            if let Err(e) = run.start(x, mode).await {
                run.problem(e);
            }
        }
    }
}

/// Replace a voter by a new id. A live one: add a learner, start it, wait
/// until it has caught up, promote it, remove the old voter, stop it. A
/// dead one (`fail`: crashed first; or one found not running), as the
/// runbook (OPERATIONS §5.9, "Replace a failed node with a new id"): every
/// voter change is refused while it does not answer except its removal, so
/// remove it first (with `force` if that leaves 2 voters), then add, start
/// and promote the replacement.
async fn replace(run: &Arc<RunShared>, draw: u64, fail: bool) {
    let Some((_, voters)) = roles(run).await else {
        return run.count("ReplaceSkipped");
    };
    let Some(old) = pick(&voters, draw) else {
        return;
    };
    let Some(new) = alloc_spare(run) else {
        return;
    };
    if fail {
        run.event(format!("node {old} fails"));
        lock(&run.disruptions).push(run.elapsed());
        run.crash(old);
    }
    let dead = !run.running().contains(&old);
    run.event(format!(
        "operator: replace {}node {old} by node {new}",
        if dead { "dead " } else { "" }
    ));
    let add = AdminRequest::AddLearner {
        id: new,
        addr: addr_of(new, 0),
        expect: None,
    };
    if dead {
        let req = AdminRequest::Remove {
            id: old,
            expect: None,
            force: voters.len() <= 3,
        };
        if !step(run, req, STEP_BUDGET).await {
            return;
        }
        retire_later(run, old, Duration::ZERO);
        let added = step(run, add, STEP_BUDGET).await;
        start_new(run, new).await;
        if added && step(run, promotion(new), STEP_BUDGET).await {
            run.count("ReplaceDeadDone");
        }
        return;
    }
    let added = step(run, add, STEP_BUDGET).await;
    start_new(run, new).await;
    if !added || !step(run, promotion(new), STEP_BUDGET).await {
        return;
    }
    if step(run, removal(old), STEP_BUDGET).await {
        run.count("ReplaceDone");
        retire_later(run, old, Duration::from_millis(draw % 2000));
    }
}

/// Replace a node by the same id with a wiped disk at a new address: wipe
/// and restart it (discovery, rejoin), then move its address.
async fn replace_wiped(run: &Arc<RunShared>, draw: u64) {
    let Some((learners, voters)) = roles(run).await else {
        return run.count("ReplaceWipedSkipped");
    };
    let all: Vec<NodeId> = learners.into_iter().chain(voters).collect();
    let Some(x) = pick(&all, draw) else {
        return;
    };
    if let Err(why) = wipe_allowed(run, x) {
        run.event(format!(
            "replace of node {x} with a wiped disk skipped ({why})"
        ));
        return run.count("WipeSkipped");
    }
    run.crash(x);
    if let Err(e) = run.start(x, StartMode::Wipe).await {
        run.problem(e);
        return;
    }
    let req = AdminRequest::SetAddr {
        id: x,
        addr: addr_of(x, run.elapsed().as_millis() as u64),
        expect: None,
        force: false,
    };
    if step(run, req, STEP_BUDGET).await {
        run.count("ReplaceWipedDone");
    }
}

/// Two changes sent at the same time: the executor lock and the
/// compare-and-set let at most one through at a time.
async fn concurrent(run: &Arc<RunShared>, draw: u64) {
    let Some((learners, voters)) = roles(run).await else {
        return;
    };
    let mut reqs = Vec::new();
    let mut r = SimRng::new(draw);
    for _ in 0..2 {
        let req = match r.range(0, 3) {
            0 => alloc_spare(run).map(|id| AdminRequest::AddLearner {
                id,
                addr: addr_of(id, 0),
                expect: None,
            }),
            1 => pick(&learners, r.next_u64()).map(promotion),
            2 => pick(&voters, r.next_u64()).map(|v| AdminRequest::SetAddr {
                id: v,
                addr: addr_of(v, run.elapsed().as_millis() as u64 + reqs.len() as u64),
                expect: None,
                force: false,
            }),
            _ => pick(&learners, r.next_u64()).map(removal),
        };
        reqs.extend(req);
    }
    let [a, b] = <[AdminRequest; 2]>::try_from(reqs).unwrap_or_else(|v| {
        let a = v.first().cloned().unwrap_or(AdminRequest::Membership);
        [a.clone(), a]
    });
    if a == AdminRequest::Membership {
        return;
    }
    run.count("ConcurrentPair");
    let (oa, ob) = tokio::join!(request(run, a.clone()), request(run, b.clone()));
    for (req, out) in [(a, oa), (b, ob)] {
        if let AdminRequest::AddLearner { id, .. } = req
            && out.may_have_run()
        {
            start_new(run, id).await;
        }
        if let AdminRequest::Remove { id, .. } = req
            && out.may_have_run()
        {
            retire_later(run, id, Duration::from_millis(draw % 2000));
        }
    }
}

/// A voter change whose leader crashes as soon as the joint configuration
/// appears in its log (between openraft's two steps): the next leader
/// finishes it towards the new voters, or it never committed.
async fn leader_loss(run: &Arc<RunShared>, draw: u64) {
    let Some((learners, voters)) = roles(run).await else {
        return run.count("LeaderLossSkipped");
    };
    let Some(leader) = run.leader() else {
        return run.count("LeaderLossSkipped");
    };
    let others: Vec<NodeId> = voters.iter().copied().filter(|&v| v != leader.id).collect();
    let req = if let Some(x) = pick(&learners, draw) {
        promotion(x)
    } else if voters.len() > 3
        && let Some(x) = pick(&others, draw)
    {
        removal(x)
    } else {
        // Nothing to change in one voter step: grow first.
        add(run, false).await;
        return run.count("LeaderLossGrew");
    };
    let l = leader.clone();
    let run2 = run.clone();
    let delay = Duration::from_millis(draw % 30);
    let watcher = tokio::spawn(async move {
        let mut rx = l.raft.server_metrics();
        let seen = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if rx
                    .borrow_and_update()
                    .membership_config
                    .membership()
                    .get_joint_config()
                    .len()
                    > 1
                {
                    return;
                }
                if rx.changed().await.is_err() {
                    std::future::pending::<()>().await;
                }
            }
        })
        .await;
        drop(rx);
        let id = l.id;
        drop(l);
        if seen.is_ok() {
            tokio::time::sleep(delay).await;
            run2.event(format!(
                "leader {id} crashes between the joint and uniform steps"
            ));
            run2.count("LeaderLossCrashed");
            lock(&run2.disruptions).push(run2.elapsed());
            run2.crash(id);
        }
    });
    drop(leader);
    let out = request(run, req.clone()).await;
    // Only this change's joint step: a later one is not this fault's.
    watcher.abort();
    if let AdminRequest::Remove { id, .. } = req
        && out.may_have_run()
    {
        retire_later(run, id, Duration::from_millis(draw % 2000));
    }
}

/// After the schedule and the heal: let the operator flows end, restart
/// every node that should run, wait until the leader's membership is
/// committed and uniform, stop every node outside it and start every
/// member. Returns the members, `None` on a failure (recorded).
pub(super) async fn settle(run: &Arc<RunShared>) -> Option<BTreeSet<NodeId>> {
    restart_all(run).await;
    // Operator tasks may start more (a removal's stop), so until none is
    // left.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let ops: Vec<_> = lock(&run.operators).drain(..).collect();
        if ops.is_empty() {
            break;
        }
        for h in ops {
            let left = deadline.saturating_duration_since(Instant::now());
            let abort = h.abort_handle();
            if tokio::time::timeout(left, h).await.is_err() {
                abort.abort();
                run.count("OperatorAborted");
            }
        }
    }
    restart_all(run).await;
    let mut members = None;
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Some((log_id, m)) = stable(run).await {
            let l = run.leader().map(|l| l.id);
            run.event(format!("settle: leader {l:?} membership {log_id:?}"));
            members = Some(m.nodes().map(|(&n, _)| n).collect::<BTreeSet<NodeId>>());
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let Some(members) = members else {
        run.problem(format!(
            "invariant 1: no leader with a committed, uniform membership 30 s after healing: {}",
            super::cluster_view(run)
        ));
        return None;
    };
    run.event(format!("settled membership: {members:?}"));
    for id in run.running() {
        if !members.contains(&id) {
            retire(run, id);
        }
    }
    let refused = lock(&run.refused).clone();
    for &id in &members {
        if refused.contains(&id) {
            run.problem(format!(
                "node {id} is a member, but its discovery refused it"
            ));
        }
        if let Err(e) = run.start(id, StartMode::Normal).await {
            run.problem(e);
        }
    }
    Some(members)
}

/// Starts every node that should run (a restart lets the cluster make
/// progress again, so it counts as a disruption for invariant 3).
async fn restart_all(run: &Arc<RunShared>) {
    lock(&run.disruptions).push(run.elapsed());
    for id in run.restartable() {
        if let Err(e) = run.start(id, StartMode::Normal).await {
            run.problem(e);
        }
    }
}

/// Invariants 1 (every member holds the settled membership), 2 and 3,
/// after the final verification.
pub(super) async fn check_final(run: &Arc<RunShared>, members: &BTreeSet<NodeId>) {
    let Some((log_id, m)) = stable(run).await else {
        run.problem("invariant 1: the membership is not settled at the end".into());
        return;
    };
    let now: BTreeSet<NodeId> = m.nodes().map(|(&n, _)| n).collect();
    if now != *members {
        run.problem(format!(
            "invariant 1: the membership changed after settling: {members:?} -> {now:?}"
        ));
    }
    let same = wait_until(Duration::from_secs(5), || {
        lock(&run.nodes)
            .values()
            .all(|n| *n.effective().log_id() == log_id)
    })
    .await;
    if !same {
        let views: Vec<(NodeId, Option<LogId<NodeId>>)> = lock(&run.nodes)
            .values()
            .map(|n| (n.id, *n.effective().log_id()))
            .collect();
        run.problem(format!(
            "invariant 1: members disagree on the membership (leader {log_id:?}): {views:?}"
        ));
    }
    let owners_ok = || {
        lock(&run.nodes).values().all(|n| {
            n.state
                .connection_owners()
                .keys()
                .all(|o| members.contains(o))
        })
    };
    if !wait_until(Duration::from_secs(5), owners_ok).await {
        let bad: Vec<(NodeId, Vec<NodeId>)> = lock(&run.nodes)
            .values()
            .map(|n| {
                (
                    n.id,
                    n.state
                        .connection_owners()
                        .into_keys()
                        .filter(|o| !members.contains(o))
                        .collect(),
                )
            })
            .collect();
        run.problem(format!(
            "invariant 2: connection owners outside the membership {members:?}: {bad:?}"
        ));
    }
    // No leaked connection: every connection in the state that a running
    // node owns belongs to its current incarnation (a previous one's would
    // stay, with its reservations, as no leader drops a live node's).
    let leaked = || -> Vec<bstk_engine::ConnId> {
        let nodes = lock(&run.nodes);
        let Some(any) = nodes.values().next() else {
            return Vec::new();
        };
        any.state
            .conn_ids()
            .into_iter()
            .filter(|&c| {
                nodes
                    .get(&bstk_raft::owner_of(c))
                    .is_some_and(|n| !lock(&n.clients).contains_key(&c))
            })
            .collect()
    };
    if !wait_until(Duration::from_secs(5), || leaked().is_empty()).await {
        run.problem(format!(
            "invariant 2: connections of earlier incarnations of running nodes left in the \
             state: {:?}",
            leaked()
        ));
    }
    let l = lock(&run.ledger);
    let disruptions = lock(&run.disruptions).clone();
    for (&x, &(index, removed_at)) in &l.removed {
        let Some(&released) = l.released.get(&x) else {
            continue;
        };
        let anchor = disruptions
            .iter()
            .copied()
            .filter(|&t| t <= released)
            .fold(removed_at, Duration::max);
        if released > anchor + RELEASE_BOUND {
            run.problem(format!(
                "invariant 3: node {x} was removed at index {index} at {removed_at:?}, but its \
                 connections were still in the state until {released:?} (more than \
                 {RELEASE_BOUND:?} after {anchor:?})"
            ));
        }
    }
}

type Mem = Membership<NodeId, BasicNode>;

/// What one uniform membership step did.
fn step_of(a: &Mem, b: &Mem) -> Result<AdminRequest, String> {
    let va: BTreeSet<NodeId> = a.voter_ids().collect();
    let vb: BTreeSet<NodeId> = b.voter_ids().collect();
    let na: BTreeMap<NodeId, String> = a.nodes().map(|(&n, x)| (n, x.addr.clone())).collect();
    let nb: BTreeMap<NodeId, String> = b.nodes().map(|(&n, x)| (n, x.addr.clone())).collect();
    let added: Vec<NodeId> = nb.keys().filter(|n| !na.contains_key(n)).copied().collect();
    let gone: Vec<NodeId> = na.keys().filter(|n| !nb.contains_key(n)).copied().collect();
    let promoted: Vec<NodeId> = vb.difference(&va).copied().collect();
    let demoted: Vec<NodeId> = va.difference(&vb).copied().collect();
    let moved: Vec<NodeId> = nb
        .iter()
        .filter(|(n, addr)| na.get(n).is_some_and(|x| x != *addr))
        .map(|(&n, _)| n)
        .collect();
    let what = format!(
        "added {added:?}, removed {gone:?}, promoted {promoted:?}, demoted {demoted:?}, moved \
         {moved:?}"
    );
    match (
        added.as_slice(),
        gone.as_slice(),
        promoted.as_slice(),
        demoted.as_slice(),
        moved.as_slice(),
    ) {
        ([x], [], [], [], []) => Ok(AdminRequest::AddLearner {
            id: *x,
            addr: nb[x].clone(),
            expect: None,
        }),
        ([], [], [x], [], []) => Ok(promotion(*x)),
        ([], [x], [], [y], []) if x == y => Ok(removal(*x)),
        ([], [x], [], [], []) => Ok(removal(*x)),
        ([], [], [], [], [x]) => Ok(AdminRequest::SetAddr {
            id: *x,
            addr: nb[x].clone(),
            expect: None,
            force: false,
        }),
        _ => Err(what),
    }
}

/// Whether two requests ask for the same change (`expect` and `force`
/// aside).
fn same_change(a: &AdminRequest, b: &AdminRequest) -> bool {
    match (a, b) {
        (
            AdminRequest::AddLearner { id: i, addr: x, .. },
            AdminRequest::AddLearner { id: j, addr: y, .. },
        )
        | (
            AdminRequest::SetAddr { id: i, addr: x, .. },
            AdminRequest::SetAddr { id: j, addr: y, .. },
        ) => i == j && x == y,
        (AdminRequest::Promote { ids: i, .. }, AdminRequest::Promote { ids: j, .. }) => i == j,
        (AdminRequest::Remove { id: i, .. }, AdminRequest::Remove { id: j, .. }) => i == j,
        _ => false,
    }
}

/// Invariants 1 and 5 on the committed log (see the module docs).
pub(super) fn check_log(run: &RunShared, initial: &[NodeId]) -> Vec<String> {
    let mut out = Vec::new();
    let l = lock(&run.ledger);
    let attempts = lock(&run.attempts).clone();
    let mut mems: Vec<(LogId<NodeId>, Mem)> = Vec::new();
    for (bytes, _) in l.entries.values() {
        if let Ok(e) = postcard::from_bytes::<Entry<TypeConfig>>(bytes)
            && let EntryPayload::Membership(m) = e.payload
        {
            mems.push((e.log_id, m));
        }
    }
    let mut prev: Option<&Mem> = None;
    let mut joint_from: Option<&Mem> = None;
    let mut ever: BTreeSet<NodeId> = BTreeSet::new();
    for (log_id, m) in &mems {
        let configs = m.get_joint_config();
        let Some(p) = prev else {
            let want: BTreeSet<NodeId> = initial.iter().copied().collect();
            if configs.len() != 1 || configs[0] != want {
                out.push(format!(
                    "invariant 1: the first membership {configs:?} is not the initial voters"
                ));
            }
            ever.extend(m.nodes().map(|(&n, _)| n));
            prev = Some(m);
            continue;
        };
        if p.get_joint_config().len() > 1 {
            // A1: a joint (C, C') is finished towards C'.
            let goal = bstk_raft::admin::joint_goal(p.get_joint_config());
            if configs.len() != 1 || Some(&configs[0]) != goal.as_ref() {
                out.push(format!(
                    "invariant 1 (A1): the joint configuration {:?} was followed at index {} by \
                     {configs:?}, not by its new half",
                    p.get_joint_config(),
                    log_id.index
                ));
            }
        } else if configs.len() > 1 {
            let (old, new) = (&configs[0], &configs[1]);
            let diff = old.symmetric_difference(new).count();
            if configs.len() != 2 || old != &p.get_joint_config()[0] || diff != 1 {
                out.push(format!(
                    "invariant 1: the joint configuration {configs:?} at index {} does not \
                     change one voter of {:?}",
                    log_id.index,
                    p.get_joint_config()
                ));
            }
            joint_from = Some(p);
            prev = Some(m);
            continue;
        }
        let from = joint_from.take().unwrap_or(p);
        match step_of(from, m) {
            Ok(change) => {
                if let AdminRequest::AddLearner { id, .. } = &change
                    && ever.iter().any(|e| e >= id)
                {
                    out.push(format!(
                        "invariant 5: node {id} was added at index {} although ids up to {:?} \
                         had been used",
                        log_id.index,
                        ever.last()
                    ));
                }
                let asked = attempts
                    .iter()
                    .any(|a| same_change(&a.req, &change) && a.outcome.may_have_run());
                if !asked {
                    out.push(format!(
                        "invariant 1: the membership change at index {} ({change:?}) was not \
                         requested by any request that was not refused",
                        log_id.index
                    ));
                }
            }
            Err(what) => out.push(format!(
                "invariant 1: unexplained membership change at index {}: {what}",
                log_id.index
            )),
        }
        ever.extend(m.nodes().map(|(&n, _)| n));
        prev = Some(m);
    }
    // Every `Done` names a committed membership with the requested effect.
    for a in &attempts {
        let Outcome::Done(Some(done)) = a.outcome else {
            continue;
        };
        match mems.iter().find(|(id, _)| id.index == done.index) {
            Some((id, m)) if *id == done => {
                if !has_effect(&a.req, m) {
                    out.push(format!(
                        "invariant 1: {:?} answered Done at {done} but that membership lacks \
                         the change",
                        a.req
                    ));
                }
            }
            _ => {
                if l.entries.contains_key(&done.index) {
                    out.push(format!(
                        "invariant 1: {:?} answered Done at {done}, which is not a committed \
                         membership entry",
                        a.req
                    ));
                }
            }
        }
    }
    let mut last: Option<(u64, NodeId)> = None;
    for (&i, &(h, _)) in &l.highest {
        if let Some((pi, ph)) = last
            && h < ph
        {
            out.push(format!(
                "invariant 5: the highest member id fell from {ph} at index {pi} to {h} at index \
                 {i}"
            ));
        }
        last = Some((i, h));
    }
    out
}

/// Which guardrail a refusal comes from (for the run's statistics).
fn guardrail(reason: &str) -> &'static str {
    const KINDS: &[(&str, &str)] = &[
        ("did not answer", "silent-node"),
        ("is rejoining", "rejoining"),
        ("not caught up", "lagging-learner"),
        ("has not replicated", "learner-never-ran"),
        ("fewer than", "min-voters"),
        ("last voter", "last-voter"),
        ("never reused", "id-reuse"),
        ("not a learner", "not-a-learner"),
        ("already a member", "already-member"),
        ("not a member", "not-a-member"),
        ("joint", "joint"),
        ("in progress", "in-progress"),
        ("one node at a time", "one-at-a-time"),
        ("raft has stopped", "raft-stopped"),
    ];
    KINDS
        .iter()
        .find(|(k, _)| reason.contains(k))
        .map_or("other", |(_, v)| v)
}

pub(super) fn stats(run: &RunShared, stats: &mut BTreeMap<String, u64>) {
    for a in lock(&run.attempts).iter() {
        let kind = match a.req {
            AdminRequest::Membership => "membership",
            AdminRequest::AddLearner { .. } => "add",
            AdminRequest::Promote { .. } => "promote",
            AdminRequest::Remove { .. } => "remove",
            AdminRequest::SetAddr { .. } => "set-addr",
        };
        let outcome = match &a.outcome {
            Outcome::Done(_) => "done",
            Outcome::Started => "started",
            Outcome::NotLeader => "not-leader",
            Outcome::Conflict => "conflict",
            Outcome::Refused(_) => "refused",
            Outcome::Lost => "lost",
            Outcome::NoLeader => "no-leader",
        };
        *stats.entry(format!("op:{kind}:{outcome}")).or_default() += 1;
        if let Outcome::Refused(r) = &a.outcome {
            *stats
                .entry(format!("refused:{}", guardrail(r)))
                .or_default() += 1;
        }
    }
    let l = lock(&run.ledger);
    if run.membership {
        stats.insert("membership-entries".into(), l.memberships.len() as u64);
        stats.insert("removed-nodes".into(), l.removed.len() as u64);
    }
}
