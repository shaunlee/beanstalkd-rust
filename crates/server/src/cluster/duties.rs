//! Time-driven cluster work.
//!
//! # Leader duties ([`leader_duties`])
//!
//! - **Timers**: followers never tick on their own. The leader watches the
//!   applied state (`StateHandle::subscribe`) and proposes `Op::Tick` when
//!   its clock reaches `next_deadline()`. Any applied entry also ticks the
//!   engine, so a `Tick` is only needed when nothing else is applied; one
//!   is re-proposed if the applied state has not moved [`TICK_RETRY`] later
//!   (a proposal lost to a leader change).
//! - **Node liveness**: the leader replicates to every follower (with
//!   heartbeats every `cluster.heartbeat` when idle) over its cluster
//!   network, which records when each peer last answered anything
//!   (`Network::last_response`). A peer that has not answered for
//!   `2 × node_timeout` (counted from when this node became leader at the
//!   earliest) is gone: if the state still holds connections it owns, the
//!   leader proposes `DropNode { node: peer, up_to_local }` with the
//!   highest local number the state has seen for it, at most once per
//!   silence period (an answer ends the period). The bound keeps a late
//!   commit from closing connections the node accepted after a restart.
//!
//! # Readiness ([`readiness`])
//!
//! Every node: ready (`/readyz` 200) once the startup cleanup is done, a
//! leader is known, and the applied index has reached the last commit index
//! this node learned.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use bstk_raft::{NodeId, Op};

use super::Core;

/// Re-propose a due `Tick` if nothing was applied for this long.
pub const TICK_RETRY: Duration = Duration::from_millis(500);
/// How often liveness and readiness are evaluated.
const PERIOD: Duration = Duration::from_millis(100);

pub async fn leader_duties(core: Arc<Core>) {
    let mut info = core.state.subscribe();
    let mut metrics = core.raft.metrics();
    let mut period = tokio::time::interval(PERIOD);
    period.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let sleep = tokio::time::sleep(Duration::ZERO);
    tokio::pin!(sleep);
    let mut leader_since: Option<Instant> = None;
    // (deadline, applied index, when) of the last Tick proposed.
    let mut last_tick: Option<(u64, Option<u64>, Instant)> = None;
    let mut dropped: BTreeSet<NodeId> = BTreeSet::new();
    loop {
        let leading = core.is_leader();
        let mut wait_for = None;
        if leading {
            leader_since.get_or_insert_with(Instant::now);
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
        }
        if let Some(at) = wait_for {
            sleep.as_mut().reset(at);
        }
        tokio::select! {
            r = info.changed() => if r.is_err() { return },
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

async fn liveness(core: &Core, leader_since: Instant, dropped: &mut BTreeSet<NodeId>) {
    let silence = core.node_timeout.saturating_mul(2);
    let mut owners: Option<HashMap<NodeId, usize>> = None;
    for &peer in &core.peers {
        if peer == core.id {
            continue;
        }
        let last = core
            .net
            .last_response(peer)
            .map_or(leader_since, |t| t.max(leader_since));
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
            // Raft has stopped.
            Err(_) => {
                core.status.ready.store(false, Ordering::Release);
                return;
            }
        };
        core.status
            .committed
            .store(committed.unwrap_or(u64::MAX), Ordering::Release);
        let caught_up = committed.is_none_or(|c| core.applied_index().is_some_and(|a| a >= c));
        let ready =
            core.status.started.load(Ordering::Acquire) && core.leader().is_some() && caught_up;
        core.status.ready.store(ready, Ordering::Release);
    }
}
