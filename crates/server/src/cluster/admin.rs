//! The leader-side membership executor behind the admin channel
//! (docs/DESIGN.md §8, "Membership changes (P6-T4)"): the executor itself is
//! `bstk_raft::admin` (shared with the in-process chaos harness, so the
//! harness exercises the same guardrails); this module makes [`Core`] its
//! host.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bstk_raft::admin::AdminHost;
use bstk_raft::forward::ForwardError;
use bstk_raft::status::NodeStatusEx;
use bstk_raft::wire::{AdminRequest, AdminResponse};
use bstk_raft::{NodeId, Op, TypeConfig};
use openraft::{BasicNode, Raft, StoredMembership};

use super::Core;

impl AdminHost for Core {
    fn id(&self) -> NodeId {
        self.id
    }

    fn raft(&self) -> &Raft<TypeConfig> {
        &self.raft
    }

    fn is_leader(&self) -> bool {
        Core::is_leader(self)
    }

    fn leader(&self) -> Option<NodeId> {
        Core::leader(self)
    }

    fn membership(&self) -> Arc<StoredMembership<NodeId, BasicNode>> {
        Core::membership(self)
    }

    fn watch_view(
        &self,
    ) -> tokio::sync::watch::Receiver<openraft::metrics::RaftServerMetrics<NodeId, BasicNode>> {
        Core::watch_view(self)
    }

    fn admin_lock(&self) -> &Arc<tokio::sync::Mutex<()>> {
        &self.admin_lock
    }

    fn highest_member(&self) -> NodeId {
        self.state.highest_member()
    }

    fn tls(&self) -> bool {
        self.tls
    }

    fn plaintext_allow_remote(&self) -> bool {
        self.plaintext_allow_remote
    }

    fn heartbeat(&self) -> Duration {
        self.heartbeat
    }

    fn applied_up_to(&self, index: u64) -> impl Future<Output = ()> + Send {
        Core::applied_up_to(self, index)
    }

    fn status_ex(
        &self,
        node: NodeId,
    ) -> impl Future<Output = Result<NodeStatusEx, ForwardError>> + Send {
        self.net.status_ex(node)
    }

    fn silent_for(&self, node: NodeId) -> Option<Duration> {
        self.net.last_response(node).map(|t| t.elapsed())
    }

    fn owns_connections(&self, node: NodeId) -> bool {
        Core::owns_connections(self, node)
    }

    fn highest_local(&self, node: NodeId) -> u64 {
        self.state.highest_local(node)
    }

    async fn propose(&self, op: Op) -> bool {
        Core::propose(self, op).await.is_ok()
    }

    async fn before_change(&self) {
        #[cfg(feature = "test-hooks")]
        super::test_hooks::hold_change(&self.data_dir).await;
    }
}

/// Serves an admin change (`bstk_raft::admin::handle` on this node).
pub async fn handle(core: Arc<Core>, req: AdminRequest, from: SocketAddr) -> AdminResponse {
    bstk_raft::admin::handle(core, req, from).await
}

/// Finishes joint configurations left by earlier leaders
/// (`bstk_raft::admin::finish_joint` on this node). Runs until Raft stops.
pub async fn finish_joint(core: Arc<Core>) {
    bstk_raft::admin::finish_joint(core).await;
}
