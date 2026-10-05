//! Detection of a fatal Raft stop (docs/DESIGN.md §8, "Fatal Raft stop").
//!
//! openraft 0.9 can shut a node's Raft core down for good while the process
//! is healthy; the known cause is a race that kills a leader (see the
//! design section). Nothing in the core recovers from it, so the owner of
//! the `Raft` has to notice and restart the node.

use openraft::error::Fatal;
use openraft::{BasicNode, RaftMetrics};
use tokio::sync::watch;

use crate::NodeId;

/// Resolves with the error that stopped the Raft core, or with `None` when
/// the node stopped normally (`Raft::shutdown`) or the metrics are gone.
pub async fn wait_fatal(
    mut metrics: watch::Receiver<RaftMetrics<NodeId, BasicNode>>,
) -> Option<Fatal<NodeId>> {
    loop {
        match &metrics.borrow_and_update().running_state {
            Err(Fatal::Stopped) => return None,
            Err(e) => return Some(e.clone()),
            Ok(()) => {}
        }
        metrics.changed().await.ok()?;
    }
}
