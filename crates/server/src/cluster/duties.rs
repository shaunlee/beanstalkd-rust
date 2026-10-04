//! Time-driven cluster work.
//!
//! # Leader duties ([`leader_duties`])
//!
//! - **Timers**: followers never tick on their own. The leader watches the
//!   applied state (`StateHandle::subscribe`) and proposes `Op::Tick` when
//!   its clock reaches `next_deadline()`. Any applied entry also ticks the
//!   engine, so a `Tick` is only needed when nothing else is applied; one is
//!   re-proposed if the applied state has not moved [`TICK_RETRY`] later (a
//!   proposal lost to a leader change).
//! - **Node liveness** (docs/DESIGN.md §8 "Node loss"): a peer is alive only
//!   if the link works both ways: it answers the leader's replication
//!   (`Network::last_response`) and sends forwards or pings
//!   (`Core::heard_from`). A peer missing either for `2 × node_timeout`
//!   (counted from when this node became leader at the earliest) is gone: the
//!   leader proposes `DropNode { node: peer, up_to_local }` with the highest
//!   local number the state has seen for it, at most once per silence period.
//!   The bound keeps a late commit from closing connections the node accepted
//!   after a restart. The peers checked are the effective membership's (voters
//!   and learners), not the config's.
//! - **Non-member owners** (docs/DESIGN.md §8, "Membership-driven
//!   networking"): every node that owns connections in the replicated state
//!   but is not in the effective membership (removed, or never added) gets a
//!   `DropNode` at once, checked whenever this loop wakes (a membership change
//!   wakes it), and again whenever its highest local number grows past the
//!   bound last proposed: forwards it sent before it was removed may still be
//!   proposed after that `DropNode` and open connections above its bound. A
//!   proposal is repeated at most every [`NON_MEMBER_RETRY`] while its
//!   connections remain.
//!
//! # Readiness ([`readiness`])
//!
//! Every node: ready (`/readyz` 200) once the startup cleanup is done, a
//! leader is known, the applied index has reached the last commit index
//! this node learned, and the node is in its effective membership (a node
//! removed while running is not told, and must not be sent clients).

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use bstk_raft::{NodeId, Op};

use super::Core;

pub const TICK_RETRY: Duration = Duration::from_millis(500);
const PERIOD: Duration = Duration::from_millis(100);
/// How soon a non-member's `DropNode` is proposed again with the same
/// bound while its connections remain (a proposal lost to a leader change
/// is covered by the new leader, which starts afresh).
pub const NON_MEMBER_RETRY: Duration = Duration::from_secs(1);

pub async fn leader_duties(core: Arc<Core>) {
    let mut info = core.state.subscribe();
    let mut metrics = core.watch_view();
    let mut period = tokio::time::interval(PERIOD);
    period.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let sleep = tokio::time::sleep(Duration::ZERO);
    tokio::pin!(sleep);
    let mut leader_since: Option<Instant> = None;
    // (deadline, applied index, when) of the last Tick proposed.
    let mut last_tick: Option<(u64, Option<u64>, Instant)> = None;
    let mut dropped: BTreeSet<NodeId> = BTreeSet::new();
    // Non-member owner -> (bound last proposed, when).
    let mut outsiders: HashMap<NodeId, (u64, Instant)> = HashMap::new();
    // (membership log id, when) of the last non-member check: run at once
    // after a membership change, else every PERIOD (this loop also wakes
    // for every applied entry).
    let mut members_checked: Option<(Option<openraft::LogId<NodeId>>, Instant)> = None;
    loop {
        let leading = core.is_leader();
        let mut wait_for = None;
        if leading {
            leader_since.get_or_insert_with(Instant::now);
            let mlog = *core.membership().log_id();
            if members_checked.is_none_or(|(l, at)| l != mlog || at.elapsed() >= PERIOD) {
                members_checked = Some((mlog, Instant::now()));
                if drop_non_members(&core, &mut outsiders).await.is_err() {
                    return;
                }
            }
            if let Some(d) = core.state.next_deadline() {
                if core.clock.now() >= d {
                    let applied = core.applied_index();
                    let fresh = last_tick.is_none_or(|(ld, la, at)| {
                        ld != d || la != applied || at.elapsed() >= TICK_RETRY
                    });
                    if fresh {
                        if core.propose(Op::Tick).await.is_err() {
                            return;
                        }
                        last_tick = Some((d, applied, Instant::now()));
                    }
                    wait_for = Some(tokio::time::Instant::now() + TICK_RETRY);
                } else {
                    wait_for = Some(tokio::time::Instant::from_std(core.clock.instant_at(d)));
                }
            }
        } else {
            leader_since = None;
            last_tick = None;
            dropped.clear();
            outsiders.clear();
            members_checked = None;
        }
        if let Some(at) = wait_for {
            sleep.as_mut().reset(at);
        }
        tokio::select! {
            // Only the leader proposes `Tick`s: a follower woken by every
            // apply would do nothing with it.
            r = info.changed(), if leading => if r.is_err() { return },
            r = metrics.changed() => if r.is_err() { return },
            () = &mut sleep, if wait_for.is_some() => {}
            _ = period.tick() => {
                if let Some(since) = leader_since && leading {
                    liveness(&core, since, &mut dropped).await;
                }
            }
        }
    }
}

/// Proposes `DropNode` for connection owners outside the effective
/// membership (see the module docs). `Err`: Raft has stopped.
async fn drop_non_members(
    core: &Core,
    outsiders: &mut HashMap<NodeId, (u64, Instant)>,
) -> Result<(), super::RaftStopped> {
    let m = core.membership();
    let owners = core.state.connection_owners();
    outsiders.retain(|n, _| owners.contains_key(n));
    for (node, highest) in owners {
        if node == core.id || super::membership::is_member(&m, node) {
            continue;
        }
        let due = outsiders
            .get(&node)
            .is_none_or(|&(bound, at)| highest > bound || at.elapsed() >= NON_MEMBER_RETRY);
        if !due {
            continue;
        }
        tracing::warn!(
            node,
            up_to_local = highest,
            "connections of a node outside the membership: proposing DropNode"
        );
        core.propose(Op::DropNode {
            node,
            up_to_local: highest,
        })
        .await?;
        core.status
            .drop_node_proposals
            .fetch_add(1, Ordering::Relaxed);
        outsiders.insert(node, (highest, Instant::now()));
    }
    Ok(())
}

async fn liveness(core: &Core, leader_since: Instant, dropped: &mut BTreeSet<NodeId>) {
    let silence = core.node_timeout.saturating_mul(2);
    let mut owners: Option<HashMap<NodeId, usize>> = None;
    let members: Vec<NodeId> = core.membership().nodes().map(|(&n, _)| n).collect();
    dropped.retain(|n| members.contains(n));
    for peer in members {
        if peer == core.id {
            continue;
        }
        let answered = core
            .net
            .last_response(peer)
            .map_or(leader_since, |t| t.max(leader_since));
        let heard = core
            .last_heard(peer)
            .map_or(leader_since, |t| t.max(leader_since));
        let last = answered.min(heard);
        if last.elapsed() < silence {
            dropped.remove(&peer);
            continue;
        }
        if dropped.contains(&peer) {
            continue;
        }
        let owners = owners.get_or_insert_with(|| {
            let mut m = HashMap::new();
            for c in core.state.conn_ids() {
                *m.entry(bstk_raft::owner_of(c)).or_insert(0) += 1;
            }
            m
        });
        let Some(&n) = owners.get(&peer) else {
            continue;
        };
        // Only the connections seen so far: should the node restart and
        // accept new ones (numbered above this) before this commits, they
        // are left alone.
        let up_to_local = core.state.highest_local(peer);
        tracing::warn!(
            node = peer,
            connections = n,
            silent_for = ?last.elapsed(),
            "node is silent: proposing DropNode"
        );
        let op = Op::DropNode {
            node: peer,
            up_to_local,
        };
        if core.propose(op).await.is_err() {
            return;
        }
        core.status
            .drop_node_proposals
            .fetch_add(1, Ordering::Relaxed);
        dropped.insert(peer);
    }
}

pub async fn readiness(core: Arc<Core>) {
    let mut period = tokio::time::interval(PERIOD);
    period.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        period.tick().await;
        let committed = match core
            .raft
            .with_raft_state(|st| st.committed.map(|c| c.index))
            .await
        {
            Ok(c) => c,
            Err(_) => {
                core.status.ready.store(false, Ordering::Release);
                return;
            }
        };
        core.status
            .committed
            .store(committed.unwrap_or(u64::MAX), Ordering::Release);
        let caught_up = committed.is_none_or(|c| core.applied_index().is_some_and(|a| a >= c));
        // An isolated node still names a leader (a partitioned openraft 0.9
        // leader names itself) but closes every client connection.
        let ready = core.status.started.load(Ordering::Acquire)
            && core.leader().is_some()
            && caught_up
            && !core.status.isolated.load(Ordering::Relaxed)
            // The effective membership, not `Core::is_member`: that one still
            // admits a node whose removal is not committed yet.
            && super::membership::is_member(&core.membership(), core.id);
        core.status.ready.store(ready, Ordering::Release);
    }
}
