#![allow(clippy::unwrap_used)]

mod ordering;
mod proptests;
mod recovery;
mod unit;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use bstk_engine::{JobRecord, JournalEntry, RecordState, RecoveredJob, Recovery};
use bstk_proto::{JobId, TubeName};
use bytes::Bytes;

use crate::{SyncPolicy, Wal, WalOptions};

pub(crate) fn opts(dir: &Path, file_size: u64) -> WalOptions {
    WalOptions {
        dir: dir.to_path_buf(),
        file_size,
        sync: SyncPolicy::Never,
    }
}

pub(crate) fn tmp() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

pub(crate) fn rec(id: JobId) -> JobRecord {
    JobRecord {
        id,
        pri: 100,
        delay: 0,
        ttr: 60,
        created_at: 1_000 + id,
        deadline_at: 0,
        state: RecordState::Ready,
        reserve_ct: 0,
        timeout_ct: 0,
        release_ct: 0,
        bury_ct: 0,
        kick_ct: 0,
    }
}

pub(crate) fn tube(name: &str) -> TubeName {
    TubeName::new(name).unwrap()
}

pub(crate) fn put(id: JobId, t: &str, body: &[u8]) -> JournalEntry {
    JournalEntry::Put {
        record: rec(id),
        tube: tube(t),
        body: Bytes::copy_from_slice(body),
    }
}

pub(crate) fn buried(id: JobId, n: u32) -> JournalEntry {
    let mut r = rec(id);
    r.state = RecordState::Buried;
    r.bury_ct = n;
    JournalEntry::Update(r)
}

/// Reserve (asserting success) and append.
pub(crate) fn write(wal: &mut Wal, entries: &[JournalEntry]) {
    for e in entries {
        if let JournalEntry::Put { tube, body, .. } = e {
            assert!(wal.reserve_put(tube.as_str().len(), body.len()));
        }
    }
    wal.append(entries).unwrap();
}

/// Segment files in the directory, by index.
pub(crate) fn seg_files(dir: &Path) -> Vec<(u64, PathBuf)> {
    let mut v: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| {
            let e = e.unwrap();
            let name = e.file_name().into_string().ok()?;
            crate::replay::segment_index(&name).map(|i| (i, e.path()))
        })
        .collect();
    v.sort();
    v
}

pub(crate) fn seg_path(dir: &Path, index: u64) -> PathBuf {
    dir.join(format!("binlog.{index}"))
}

/// Copy every regular file of `src` into a new temp dir.
pub(crate) fn copy_dir(src: &Path) -> tempfile::TempDir {
    let d = tmp();
    for e in std::fs::read_dir(src).unwrap() {
        let e = e.unwrap();
        if e.file_type().unwrap().is_file() {
            std::fs::copy(e.path(), d.path().join(e.file_name())).unwrap();
        }
    }
    d
}

/// Simple in-memory model of the journal: live jobs in first-put order,
/// and the reference's replay tube list over every record written.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Model {
    pub jobs: BTreeMap<JobId, RecoveredJob>,
    pub order: Vec<JobId>,
    pub max_id: JobId,
    pub tubes: Vec<TubeName>,
}

impl Model {
    pub fn apply(&mut self, e: &JournalEntry) {
        match e {
            JournalEntry::Put { record, tube, body } => {
                self.max_id = self.max_id.max(record.id);
                if self
                    .jobs
                    .insert(
                        record.id,
                        RecoveredJob {
                            record: record.clone(),
                            tube: tube.clone(),
                            body: body.clone(),
                        },
                    )
                    .is_none()
                {
                    self.order.push(record.id);
                    if tube.as_str() != "default" && !self.tubes.contains(tube) {
                        self.tubes.push(tube.clone());
                    }
                }
            }
            JournalEntry::Update(r) => {
                self.max_id = self.max_id.max(r.id);
                if let Some(j) = self.jobs.get_mut(&r.id) {
                    j.record = r.clone();
                }
            }
            JournalEntry::Delete(id) => {
                self.max_id = self.max_id.max(*id);
                if let Some(j) = self.jobs.remove(id) {
                    self.order.retain(|x| x != id);
                    if !self.jobs.values().any(|x| x.tube == j.tube)
                        && let Some(p) = self.tubes.iter().position(|t| *t == j.tube)
                    {
                        self.tubes.swap_remove(p);
                    }
                }
            }
        }
    }

    pub fn expected_jobs(&self) -> Vec<RecoveredJob> {
        self.order.iter().map(|id| self.jobs[id].clone()).collect()
    }

    /// Same live jobs with the same contents, in any order.
    pub fn same_set(&self, r: &Recovery) -> bool {
        r.jobs.len() == self.jobs.len()
            && r.jobs
                .iter()
                .all(|j| self.jobs.get(&j.record.id) == Some(j))
    }

    pub fn from_recovery(r: &Recovery) -> Model {
        Model {
            jobs: r.jobs.iter().map(|j| (j.record.id, j.clone())).collect(),
            order: r.jobs.iter().map(|j| j.record.id).collect(),
            max_id: r.next_id - 1,
            tubes: r.tube_order.clone(),
        }
    }
}
