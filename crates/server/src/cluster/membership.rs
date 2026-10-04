//! Membership-driven networking (docs/DESIGN.md §8, "Membership-driven
//! networking (P6-T1)"): once a node has a Raft membership, the effective
//! membership (the latest membership entry in its log, committed or not,
//! both halves of a joint configuration, voters and learners) decides whom
//! the cluster listener accepts and where peers are dialed. The config's
//! `[[cluster.peer]]` list is a set of seeds (used while the node has no
//! membership) and of local address overrides (a peer listed there is always
//! dialed at the configured address, as the per-link proxies of the tests
//! need).
//!
//! [`watch`] follows openraft's server metrics (published when the
//! membership changes) and updates the dialer's address book
//! (`Network::set_members`) and the listener's allowlist (which closes the
//! connections of nodes that left).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bstk_raft::NodeId;
use bstk_raft::listener::PeerAllowlist;
use openraft::{BasicNode, StoredMembership};

use super::Core;

pub type Membership = StoredMembership<NodeId, BasicNode>;

/// Every node of `m` (voters and learners of every config) with its
/// address.
pub fn addresses(m: &Membership) -> BTreeMap<NodeId, String> {
    m.nodes().map(|(&id, n)| (id, n.addr.clone())).collect()
}

/// Whether `m` is a membership at all: a node that never received one (no
/// state yet, or rejoining before its first entries) has an empty one.
pub fn has_members(m: &Membership) -> bool {
    m.nodes().next().is_some()
}

/// Who may connect: every node of the effective membership once there is
/// one, the config seeds before.
pub fn allowed(m: &Membership, seeds: &BTreeSet<NodeId>) -> BTreeSet<NodeId> {
    if has_members(m) {
        m.nodes().map(|(&id, _)| id).collect()
    } else {
        seeds.clone()
    }
}

/// Whether `id` is a node (voter or learner) of `m`.
pub fn is_member(m: &Membership, id: NodeId) -> bool {
    m.nodes().any(|(&n, _)| n == id)
}

/// Voters of the effective configuration: in a joint configuration, the
/// larger half (quorum needs both, and the reachability check only asks
/// whether a single-voter shortcut applies).
pub fn voter_count(m: &Membership) -> usize {
    m.membership()
        .get_joint_config()
        .iter()
        .map(BTreeSet::len)
        .max()
        .unwrap_or(0)
}

/// Differences between the config's peer list and the membership worth a
/// warning: config overrides whose address differs from the membership's
/// (the override wins), and the two id sets.
pub fn config_differences(
    overrides: &BTreeMap<NodeId, String>,
    members: &BTreeMap<NodeId, String>,
) -> Vec<String> {
    let mut out = Vec::new();
    for (id, cfg_addr) in overrides {
        if let Some(m_addr) = members.get(id)
            && m_addr != cfg_addr
        {
            out.push(format!(
                "node {id}: config address {cfg_addr} overrides membership address {m_addr}"
            ));
        }
    }
    let not_members: Vec<_> = overrides
        .keys()
        .filter(|id| !members.contains_key(id))
        .collect();
    if !not_members.is_empty() {
        out.push(format!(
            "config peers not in the membership: {not_members:?}"
        ));
    }
    let not_configured: Vec<_> = members
        .keys()
        .filter(|id| !overrides.contains_key(id))
        .collect();
    if !not_configured.is_empty() {
        out.push(format!(
            "members not in the config (dialed at their membership address): {not_configured:?}"
        ));
    }
    out
}

/// Applies `m` to the address book and the allowlist; warns about config
/// differences when they changed since `last_warned`.
fn apply(
    core: &Core,
    allowlist: &PeerAllowlist,
    seeds: &BTreeSet<NodeId>,
    overrides: &BTreeMap<NodeId, String>,
    m: &Membership,
    last_warned: &mut Vec<String>,
) {
    if !has_members(m) {
        return;
    }
    let addrs = addresses(m);
    let diffs = config_differences(overrides, &addrs);
    if diffs != *last_warned {
        for d in &diffs {
            tracing::warn!(membership = ?m.log_id(), "cluster config differs from the membership: {d}");
        }
        *last_warned = diffs;
    }
    core.net.set_members(addrs);
    let allow = allowed(m, seeds);
    if allow != allowlist.get() {
        tracing::info!(membership = ?m.log_id(), allowed = ?allow, "cluster allowlist updated");
        allowlist.set(allow);
    }
}

/// Follows the effective membership until Raft stops (see the module docs).
pub async fn watch(
    core: Arc<Core>,
    allowlist: PeerAllowlist,
    seeds: BTreeSet<NodeId>,
    overrides: BTreeMap<NodeId, String>,
) {
    let mut rx = core.watch_view();
    let mut seen: Option<Membership> = None;
    let mut last_warned = Vec::new();
    loop {
        let m = rx.borrow_and_update().membership_config.clone();
        if seen.as_ref() != Some(&*m) {
            apply(&core, &allowlist, &seeds, &overrides, &m, &mut last_warned);
            seen = Some((*m).clone());
        }
        if rx.changed().await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use openraft::{LogId, Membership as RaftMembership};

    fn nodes(ids: &[NodeId]) -> BTreeMap<NodeId, BasicNode> {
        ids.iter()
            .map(|&i| (i, BasicNode::new(format!("10.0.0.{i}:11400"))))
            .collect()
    }

    fn stored(configs: Vec<BTreeSet<NodeId>>, all: &[NodeId]) -> Membership {
        let m = RaftMembership::new(configs, nodes(all));
        StoredMembership::new(
            Some(LogId::new(openraft::CommittedLeaderId::new(1, 1), 5)),
            m,
        )
    }

    fn set(ids: &[NodeId]) -> BTreeSet<NodeId> {
        ids.iter().copied().collect()
    }

    #[test]
    fn allowlist_is_seeds_without_membership_then_every_node() {
        let seeds = set(&[1, 2, 3]);
        assert_eq!(allowed(&Membership::default(), &seeds), seeds);

        // Voters 1..=3 and learner 4.
        let m = stored(vec![set(&[1, 2, 3])], &[1, 2, 3, 4]);
        assert_eq!(allowed(&m, &seeds), set(&[1, 2, 3, 4]));
        assert!(is_member(&m, 4) && !is_member(&m, 5));
        assert_eq!(voter_count(&m), 3);
    }

    #[test]
    fn joint_configs_allow_both_halves() {
        // Replacing 3 with 5: joint {1,2,3} + {1,2,5}, then {1,2,5}.
        let joint = stored(vec![set(&[1, 2, 3]), set(&[1, 2, 5])], &[1, 2, 3, 5]);
        assert_eq!(allowed(&joint, &set(&[1, 2, 3])), set(&[1, 2, 3, 5]));
        assert!(is_member(&joint, 3) && is_member(&joint, 5));
        assert_eq!(voter_count(&joint), 3);

        let after = stored(vec![set(&[1, 2, 5])], &[1, 2, 5]);
        assert_eq!(allowed(&after, &set(&[1, 2, 3])), set(&[1, 2, 5]));
        assert!(!is_member(&after, 3));

        // Shrinking 3 -> 1 through a joint config: the larger half counts.
        let shrink = stored(vec![set(&[1, 2, 3]), set(&[1])], &[1, 2, 3]);
        assert_eq!(voter_count(&shrink), 3);
        assert_eq!(voter_count(&stored(vec![set(&[1])], &[1])), 1);
    }

    #[test]
    fn config_differences_name_overrides_and_missing_ids() {
        let members: BTreeMap<NodeId, String> = [(1, "a:1"), (2, "b:1"), (4, "d:1")]
            .into_iter()
            .map(|(i, a)| (i, a.to_string()))
            .collect();
        let overrides: BTreeMap<NodeId, String> = [(1, "a:1"), (2, "proxy:9"), (3, "c:1")]
            .into_iter()
            .map(|(i, a)| (i, a.to_string()))
            .collect();
        let d = config_differences(&overrides, &members);
        assert_eq!(d.len(), 3, "{d:?}");
        assert!(d[0].contains("node 2") && d[0].contains("proxy:9"), "{d:?}");
        assert!(d[1].contains("[3]"), "{d:?}");
        assert!(d[2].contains("[4]"), "{d:?}");
        assert!(config_differences(&members, &members).is_empty());
    }
}
