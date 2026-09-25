//! A minimal in-memory Raft log store and state machine for the network
//! tests (test code only; the real storage is `crate::storage`, P3-T2).
//! The state machine records every applied `Request` in order.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex, MutexGuard};

use openraft::storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    Entry, EntryPayload, LogId, RaftLogReader, RaftSnapshotBuilder, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership, Vote,
};

use crate::{Applied, NodeId, Request, TypeConfig};

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

#[derive(Default)]
struct LogData {
    vote: Option<Vote<NodeId>>,
    committed: Option<LogId<NodeId>>,
    last_purged: Option<LogId<NodeId>>,
    log: BTreeMap<u64, Entry<TypeConfig>>,
}

#[derive(Clone, Default)]
pub struct MemLog {
    inner: Arc<Mutex<LogData>>,
}

impl RaftLogReader<TypeConfig> for MemLog {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        Ok(lock(&self.inner)
            .log
            .range(range)
            .map(|(_, e)| e.clone())
            .collect())
    }
}

impl RaftLogStorage<TypeConfig> for MemLog {
    type LogReader = MemLog;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let d = lock(&self.inner);
        let last = d.log.values().next_back().map(|e| e.log_id);
        Ok(LogState {
            last_purged_log_id: d.last_purged,
            last_log_id: last.or(d.last_purged),
        })
    }

    async fn get_log_reader(&mut self) -> MemLog {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        lock(&self.inner).vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(lock(&self.inner).vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        lock(&self.inner).committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        Ok(lock(&self.inner).committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        {
            let mut d = lock(&self.inner);
            for e in entries {
                d.log.insert(e.log_id.index, e);
            }
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        lock(&self.inner).log.split_off(&log_id.index);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut d = lock(&self.inner);
        d.log = d.log.split_off(&(log_id.index + 1));
        d.last_purged = Some(log_id);
        Ok(())
    }
}

#[derive(Default)]
struct SmData {
    last_applied: Option<LogId<NodeId>>,
    membership: StoredMembership<NodeId, openraft::BasicNode>,
    applied: Vec<Request>,
    snapshot: Option<(SnapshotMeta<NodeId, openraft::BasicNode>, Vec<u8>)>,
    snapshots_built: u64,
}

/// Records applied requests; cloning shares the state (for inspection).
#[derive(Clone, Default)]
pub struct MemSm {
    inner: Arc<Mutex<SmData>>,
}

impl MemSm {
    pub fn applied(&self) -> Vec<Request> {
        lock(&self.inner).applied.clone()
    }
}

impl RaftSnapshotBuilder<TypeConfig> for MemSm {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let mut d = lock(&self.inner);
        let data = postcard::to_stdvec(&d.applied)
            .map_err(|e| StorageIOError::write_snapshot(None, &e))?;
        d.snapshots_built += 1;
        let meta = SnapshotMeta {
            last_log_id: d.last_applied,
            last_membership: d.membership.clone(),
            snapshot_id: format!(
                "{}-{}",
                d.last_applied.map_or(0, |l| l.index),
                d.snapshots_built
            ),
        };
        d.snapshot = Some((meta.clone(), data.clone()));
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for MemSm {
    type SnapshotBuilder = MemSm;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<NodeId>>,
            StoredMembership<NodeId, openraft::BasicNode>,
        ),
        StorageError<NodeId>,
    > {
        let d = lock(&self.inner);
        Ok((d.last_applied, d.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Applied>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut d = lock(&self.inner);
        let mut out = Vec::new();
        for e in entries {
            d.last_applied = Some(e.log_id);
            match e.payload {
                EntryPayload::Blank => {}
                EntryPayload::Normal(r) => d.applied.push(r),
                EntryPayload::Membership(m) => {
                    d.membership = StoredMembership::new(Some(e.log_id), m);
                }
            }
            out.push(Applied::default());
        }
        Ok(out)
    }

    async fn get_snapshot_builder(&mut self) -> MemSm {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();
        let applied: Vec<Request> = postcard::from_bytes(&data)
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
        let mut d = lock(&self.inner);
        d.applied = applied;
        d.last_applied = meta.last_log_id;
        d.membership = meta.last_membership.clone();
        d.snapshot = Some((meta.clone(), data));
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        Ok(lock(&self.inner).snapshot.as_ref().map(|(m, d)| Snapshot {
            meta: m.clone(),
            snapshot: Box::new(Cursor::new(d.clone())),
        }))
    }
}
