//! openraft's storage conformance suite.

use openraft::StorageError;
use openraft::testing::{StoreBuilder, Suite};
use tempfile::TempDir;

use super::*;

struct Builder;

impl StoreBuilder<TypeConfig, LogStore, ClusterStateMachine, TempDir> for Builder {
    async fn build(
        &self,
    ) -> Result<(TempDir, LogStore, ClusterStateMachine), StorageError<NodeId>> {
        let d = tempfile::tempdir().expect("tempdir");
        let (log, sm) = crate::storage::open(
            d.path(),
            LogOptions {
                segment_size: 256,
                max_read_bytes: 1 << 20,
            },
            sm_opts(1, Arc::new(RecSink::default())),
        )
        .expect("open storage");
        Ok((d, log, sm))
    }
}

#[test]
fn openraft_suite() {
    Suite::test_all(Builder).unwrap();
}
