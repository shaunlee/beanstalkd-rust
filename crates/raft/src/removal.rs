//! The watch that ends a node the cluster has removed (docs/DESIGN.md §8
//! "A removed node stops by itself"): a node is not told of its removal,
//! so it asks the cluster, but only while it has a reason to think it was
//! removed, and acts only on [`status::removal_confirmed`].

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::time::Duration;

use openraft::{BasicNode, StoredMembership};
use tokio::time::Instant;

use crate::NodeId;
use crate::status::{self, NodeStatusEx};

/// How often the triggers are looked at.
const TICK: Duration = Duration::from_millis(500);
/// Probe rounds while a trigger holds: the first one at once, then at
/// growing intervals up to the maximum, so a node that cannot be confirmed
/// (a partition, a removal that is not committed) stays cheap.
const PROBE_MIN: Duration = Duration::from_secs(1);
const PROBE_MAX: Duration = Duration::from_secs(5);
/// A refusal by a peer ("not a member") keeps the watch probing this long.
const REFUSAL_WINDOW: Duration = Duration::from_secs(10);

/// What the watch needs from its node.
pub trait RemovalHost: Send + Sync {
    /// The effective membership (the latest in this node's log).
    fn effective(&self) -> StoredMembership<NodeId, BasicNode>;
    /// How many peer hellos were refused as "not a member", so far.
    fn refusals(&self) -> u64;
    /// No leader has been reachable for the node timeout: a removed node
    /// whose last leader is gone is refused by nobody, so this also makes
    /// it ask.
    fn isolated(&self) -> bool;
    /// Addresses of nodes learned from the cluster, for the next probes.
    fn learned(&self, nodes: &BTreeMap<NodeId, String>);
    /// `StatusEx` of `targets`, the answers that arrived.
    fn probe(
        &self,
        targets: &BTreeSet<NodeId>,
    ) -> impl Future<Output = BTreeMap<NodeId, NodeStatusEx>> + Send;
}

/// Whether the node's own membership suggests it is out or on its way out:
/// a membership that does not list it, or a joint one that lists it only in
/// an old half. Only a reason to ask; never a reason to exit.
pub fn may_be_removed(m: &StoredMembership<NodeId, BasicNode>, id: NodeId) -> bool {
    let configs = m.membership().get_joint_config();
    if configs.is_empty() {
        return false;
    }
    if !m.nodes().any(|(&n, _)| n == id) {
        return true;
    }
    match configs.split_last() {
        Some((last, older)) => !last.contains(&id) && older.iter().any(|c| c.contains(&id)),
        None => false,
    }
}

/// Runs until the cluster confirms that node `id` was removed and returns
/// the reason. `seeds` are the nodes asked besides the node's own members
/// and the members the answers name (a node far behind may know no current
/// voter).
pub async fn watch<H: RemovalHost>(host: &H, id: NodeId, seeds: BTreeSet<NodeId>) -> String {
    let mut known: BTreeSet<NodeId> = seeds;
    known.remove(&id);
    let mut refusals = host.refusals();
    let mut refused_until: Option<Instant> = None;
    let mut next_probe = Instant::now();
    let mut delay = PROBE_MIN;
    let mut logged: Option<Instant> = None;
    loop {
        tokio::time::sleep(TICK).await;
        let now = Instant::now();
        let seen = host.refusals();
        if seen != refusals {
            refusals = seen;
            refused_until = Some(now + REFUSAL_WINDOW);
        }
        let own = host.effective();
        let suspected = may_be_removed(&own, id);
        let refused = refused_until.is_some_and(|t| now < t);
        let isolated = host.isolated();
        if !(suspected || refused || isolated) {
            delay = PROBE_MIN;
            next_probe = now;
            continue;
        }
        if now < next_probe {
            continue;
        }
        next_probe = now + delay;
        delay = (delay * 2).min(PROBE_MAX);
        known.extend(own.nodes().map(|(&n, _)| n).filter(|&n| n != id));
        let why = if suspected {
            "it is not in its own membership"
        } else if refused {
            "peers refuse it as not a member"
        } else {
            "no leader is reachable"
        };
        if logged.is_none_or(|t| now.duration_since(t) >= Duration::from_secs(60)) {
            tracing::warn!(
                "this node may have been removed from the cluster ({why}): asking the cluster \
                 to confirm"
            );
            logged = Some(now);
        }
        let answers = host.probe(&known).await;
        if let Some(m) = status::learned_membership(&answers) {
            known.extend(m.nodes.keys().copied().filter(|&n| n != id));
            host.learned(&m.nodes);
        }
        if let Some(reason) = status::removal_confirmed(id, &answers) {
            return reason;
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use openraft::{CommittedLeaderId, LogId, Membership};

    fn stored(configs: &[&[NodeId]], learners: &[NodeId]) -> StoredMembership<NodeId, BasicNode> {
        let sets: Vec<BTreeSet<NodeId>> = configs
            .iter()
            .map(|c| c.iter().copied().collect())
            .collect();
        let nodes: BTreeMap<NodeId, BasicNode> = sets
            .iter()
            .flatten()
            .chain(learners)
            .map(|&n| (n, BasicNode::new(format!("10.0.0.{n}:1"))))
            .collect();
        StoredMembership::new(
            Some(LogId::new(CommittedLeaderId::new(1, 1), 3)),
            Membership::new(sets, nodes),
        )
    }

    #[test]
    fn only_a_membership_without_the_node_makes_it_suspect_itself() {
        assert!(!may_be_removed(&stored(&[&[1, 2, 3]], &[]), 3));
        assert!(may_be_removed(&stored(&[&[1, 2]], &[]), 3));
        // A learner is a member.
        assert!(!may_be_removed(&stored(&[&[1, 2]], &[3]), 3));
        // Leaving in a joint configuration: in the old half only.
        assert!(may_be_removed(&stored(&[&[1, 2, 3], &[1, 2]], &[]), 3));
        assert!(!may_be_removed(&stored(&[&[1, 2], &[1, 2, 3]], &[]), 3));
        // No membership at all (nothing to doubt yet).
        assert!(!may_be_removed(&StoredMembership::default(), 3));
    }

    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::status::MembershipView;

    /// A host with scripted triggers and answers; counts the probe rounds.
    struct Fake {
        own: Mutex<StoredMembership<NodeId, BasicNode>>,
        refusals: AtomicU64,
        isolated: std::sync::atomic::AtomicBool,
        answers: Mutex<BTreeMap<NodeId, NodeStatusEx>>,
        rounds: AtomicU64,
    }

    impl RemovalHost for Fake {
        fn effective(&self) -> StoredMembership<NodeId, BasicNode> {
            self.own.lock().unwrap().clone()
        }
        fn refusals(&self) -> u64 {
            self.refusals.load(Ordering::Relaxed)
        }
        fn isolated(&self) -> bool {
            self.isolated.load(Ordering::Relaxed)
        }
        fn learned(&self, _nodes: &BTreeMap<NodeId, String>) {}
        async fn probe(&self, _targets: &BTreeSet<NodeId>) -> BTreeMap<NodeId, NodeStatusEx> {
            self.rounds.fetch_add(1, Ordering::Relaxed);
            self.answers.lock().unwrap().clone()
        }
    }

    fn answer(m: &StoredMembership<NodeId, BasicNode>) -> NodeStatusEx {
        NodeStatusEx {
            raft_running: true,
            highest_member: 9,
            membership: MembershipView::from_stored(m, true),
            ..NodeStatusEx::default()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn the_watch_asks_only_while_it_has_a_reason_and_exits_on_confirmation() {
        let current = stored(&[&[1, 2, 4]], &[]);
        let fake = Fake {
            own: Mutex::new(stored(&[&[1, 2, 3, 4]], &[])),
            refusals: AtomicU64::new(0),
            isolated: std::sync::atomic::AtomicBool::new(false),
            answers: Mutex::new(BTreeMap::new()),
            rounds: AtomicU64::new(0),
        };
        let seeds: BTreeSet<NodeId> = [1, 2, 4].into();
        let watch = watch(&fake, 3, seeds);
        tokio::pin!(watch);
        // A member with no reason to doubt: never probes, never exits.
        let idle = tokio::time::timeout(Duration::from_secs(120), &mut watch).await;
        assert!(idle.is_err());
        assert_eq!(fake.rounds.load(Ordering::Relaxed), 0);

        // Refused by a peer, but nobody can confirm: probes, with backoff,
        // for the refusal window only; no exit.
        fake.refusals.store(1, Ordering::Relaxed);
        let asked = tokio::time::timeout(Duration::from_secs(30), &mut watch).await;
        assert!(asked.is_err());
        let rounds = fake.rounds.load(Ordering::Relaxed);
        assert!((3..=6).contains(&rounds), "{rounds}");

        // The voters now report a committed membership without it.
        *fake.answers.lock().unwrap() = [(1, answer(&current)), (2, answer(&current))].into();
        fake.isolated.store(true, Ordering::Relaxed);
        let reason = tokio::time::timeout(Duration::from_secs(30), &mut watch)
            .await
            .expect("confirmed");
        assert!(reason.contains("never reused"), "{reason}");
    }
}
