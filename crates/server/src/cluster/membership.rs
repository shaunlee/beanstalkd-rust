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
//! connections of nodes that left). A node removed by an entry this node has
//! appended but not committed is still admitted (P6-T4): the allowlist is
//! the effective membership's nodes plus the committed membership's, so a
//! removed leader can still tell the others that its removal is committed,
//! and an entry that is truncated never cut anyone off. The listener also
//! admits every id above [`admit_above`]: such a node was added by entries
//! this node has not seen, and may lead a cluster whose membership moved on
//! while this node was down (docs/DESIGN.md §8 "Allowlist").

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

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

/// Who may connect: every node of the effective membership and of the
/// committed one (`committed`; the same, or older, while a membership entry
/// is uncommitted) once there is one, the config seeds before.
pub fn allowed(
    m: &Membership,
    committed: &Membership,
    seeds: &BTreeSet<NodeId>,
) -> BTreeSet<NodeId> {
    if has_members(m) {
        m.nodes()
            .chain(committed.nodes())
            .map(|(&id, _)| id)
            .collect()
    } else {
        seeds.clone()
    }
}

/// The highest id this node knows was ever a member: its applied
/// `highest_member`, raised to the ids of `m` and `committed` (entries
/// appended or committed but not applied yet). Ids are never reused and new
/// ones are always above every earlier member, so an id above it can only
/// come from entries this node has not received.
pub fn admit_above(m: &Membership, committed: &Membership, highest_member: NodeId) -> NodeId {
    m.nodes()
        .chain(committed.nodes())
        .map(|(&id, _)| id)
        .fold(highest_member, NodeId::max)
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
    (m, committed): (&Membership, &Membership),
    last_warned: &mut Vec<String>,
) {
    if !has_members(m) {
        return;
    }
    let diffs = config_differences(overrides, &addresses(m));
    if diffs != *last_warned {
        for d in &diffs {
            tracing::warn!(membership = ?m.log_id(), "cluster config differs from the membership: {d}");
        }
        *last_warned = diffs;
    }
    // A node only in the committed membership keeps its address until its
    // removal is committed; the effective membership's address wins.
    let mut addrs = addresses(committed);
    addrs.extend(addresses(m));
    core.net.set_members(addrs);
    let allow = allowed(m, committed, seeds);
    core.set_admitted(allow.clone());
    let above = admit_above(m, committed, core.state.highest_member());
    if allow != allowlist.get() || Some(above) != allowlist.floor() {
        tracing::info!(
            membership = ?m.log_id(),
            committed = ?committed.log_id(),
            allowed = ?allow,
            admit_above = above,
            "cluster allowlist updated"
        );
        allowlist.set_with_floor(allow, Some(above));
    }
}

/// The committed membership in openraft's state (`None`: Raft stopped).
async fn committed(core: &Core) -> Option<Membership> {
    core.raft
        .with_raft_state(|st| {
            let c = st.membership_state.committed();
            Membership::new(*c.log_id(), c.membership().clone())
        })
        .await
        .ok()
}

/// How often [`watch`] looks again while the effective membership is not
/// committed (a commit does not change the server metrics it wakes on).
const UNCOMMITTED_POLL: Duration = Duration::from_millis(50);

/// Follows the effective membership until Raft stops (see the module docs).
pub async fn watch(
    core: Arc<Core>,
    allowlist: PeerAllowlist,
    seeds: BTreeSet<NodeId>,
    overrides: BTreeMap<NodeId, String>,
) {
    let mut rx = core.watch_view();
    let mut seen: Option<(Membership, Membership)> = None;
    let mut last_warned = Vec::new();
    loop {
        let m = rx.borrow_and_update().membership_config.clone();
        let pending = seen.as_ref().is_some_and(|(e, c)| e.log_id() != c.log_id());
        if pending || seen.as_ref().is_none_or(|(e, _)| e != &*m) {
            let Some(c) = committed(&core).await else {
                return;
            };
            if seen.as_ref() != Some(&((*m).clone(), c.clone())) {
                apply(
                    &core,
                    &allowlist,
                    &seeds,
                    &overrides,
                    (&m, &c),
                    &mut last_warned,
                );
            }
            seen = Some(((*m).clone(), c));
        }
        let pending = seen.as_ref().is_some_and(|(e, c)| e.log_id() != c.log_id());
        tokio::select! {
            r = rx.changed() => if r.is_err() { return },
            () = tokio::time::sleep(UNCOMMITTED_POLL), if pending => {}
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
        let none = Membership::default();
        assert_eq!(allowed(&none, &none, &seeds), seeds);

        // Voters 1..=3 and learner 4.
        let m = stored(vec![set(&[1, 2, 3])], &[1, 2, 3, 4]);
        assert_eq!(allowed(&m, &m, &seeds), set(&[1, 2, 3, 4]));
        assert_eq!(allowed(&m, &none, &seeds), set(&[1, 2, 3, 4]));
        assert!(is_member(&m, 4) && !is_member(&m, 5));
        assert_eq!(voter_count(&m), 3);
        // Ids above every member this node has seen may be newer members.
        assert_eq!(admit_above(&m, &none, 2), 4);
        assert_eq!(admit_above(&m, &m, 7), 7);
    }

    #[test]
    fn joint_configs_allow_both_halves() {
        // Replacing 3 with 5: joint {1,2,3} + {1,2,5}, then {1,2,5}.
        let joint = stored(vec![set(&[1, 2, 3]), set(&[1, 2, 5])], &[1, 2, 3, 5]);
        assert_eq!(
            allowed(&joint, &joint, &set(&[1, 2, 3])),
            set(&[1, 2, 3, 5])
        );
        assert!(is_member(&joint, 3) && is_member(&joint, 5));
        assert_eq!(voter_count(&joint), 3);

        let after = stored(vec![set(&[1, 2, 5])], &[1, 2, 5]);
        // Node 3 stays admitted until its removal is committed here.
        assert_eq!(
            allowed(&after, &joint, &set(&[1, 2, 3])),
            set(&[1, 2, 3, 5])
        );
        assert_eq!(allowed(&after, &after, &set(&[1, 2, 3])), set(&[1, 2, 5]));
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
