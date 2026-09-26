//! Group commit for the log store (P3-FD): `append` writes its records
//! and queues its openraft callback here; a flush worker `fdatasync`s
//! every file written since its last sync once, then invokes all the
//! callbacks it covered, in queue order.
//!
//! # Worker
//!
//! At most one worker runs at a time. It is started when a job is queued
//! and none is running, takes every queued job, syncs the distinct files
//! they name (one `fdatasync` each, usually just the last segment), invokes
//! their callbacks, and repeats until the queue is empty, then exits. So
//! jobs queued while a sync is in progress are covered by the next one.
//! The worker runs on tokio's blocking pool (`spawn_blocking`), or on a new
//! thread outside a runtime: never on the caller's task. (Being a blocking
//! task also keeps a paused test clock from jumping ahead while a sync is
//! in progress, as it did not when syncs were inline; see the chaos
//! harness.)
//!
//! # Order and durability
//!
//! - A job is queued after its records were written (under the log
//!   store's lock), and a sync of a file covers every write to it that
//!   completed before the sync started. The worker syncs a job's file
//!   after taking the job, so a callback is invoked only after its
//!   entries are durable.
//! - Callbacks are invoked in queue order, which is append order.
//! - A failed sync fails every callback it covered, and every later
//!   append (sticky): after a failed `fdatasync` the state of the written
//!   pages is unknown.
//! - [`Flusher::barrier`] waits until every job queued before it has been
//!   synced and its callback invoked. `truncate`, `purge` and `save_vote`
//!   call it first, so no sync is pending while they change the files:
//!   nothing is acknowledged after it was cut off, and a truncated or
//!   deleted segment is never synced afterwards.

use std::collections::VecDeque;
use std::fs::File;
use std::io;
use std::sync::{Arc, Mutex, MutexGuard};

use openraft::storage::LogFlushed;
use tokio::sync::oneshot;

use super::fsutil;
use crate::TypeConfig;

enum Job {
    /// Sync `file` (if any), then invoke `callback`.
    Sync {
        file: Option<Arc<File>>,
        callback: LogFlushed<TypeConfig>,
    },
    /// Everything queued before has been handled.
    Barrier(oneshot::Sender<()>),
}

#[derive(Default)]
struct State {
    queue: VecDeque<Job>,
    running: bool,
    /// A sync failed (sticky).
    failed: Option<String>,
    /// `fdatasync` calls made (tests).
    #[cfg(test)]
    syncs: u64,
    /// Workers take nothing while set (tests).
    #[cfg(test)]
    paused: bool,
}

/// The flush queue of one log store (cheap to clone; clones share it).
#[derive(Clone, Default)]
pub(crate) struct Flusher {
    state: Arc<Mutex<State>>,
}

impl std::fmt::Debug for Flusher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let st = lock(&self.state);
        f.debug_struct("Flusher")
            .field("queued", &st.queue.len())
            .field("running", &st.running)
            .field("failed", &st.failed)
            .finish()
    }
}

fn lock(m: &Mutex<State>) -> MutexGuard<'_, State> {
    // The state stays consistent across a panic (only queue operations
    // happen under the lock).
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Flusher {
    /// The error of an earlier failed sync, if any.
    pub(crate) fn failed(&self) -> Option<io::Error> {
        lock(&self.state)
            .failed
            .as_ref()
            .map(|m| io::Error::other(format!("an earlier log sync failed: {m}")))
    }

    /// Queues `callback`, to be invoked once `file` (the segment the
    /// append wrote to, if any) is synced.
    pub(crate) fn submit(&self, file: Option<Arc<File>>, callback: LogFlushed<TypeConfig>) {
        self.push(Job::Sync { file, callback });
    }

    /// Waits until every job queued before this call is done.
    pub(crate) async fn barrier(&self) {
        let (tx, rx) = oneshot::channel();
        self.push(Job::Barrier(tx));
        let _ = rx.await;
    }

    fn push(&self, job: Job) {
        let mut st = lock(&self.state);
        st.queue.push_back(job);
        #[cfg(test)]
        if st.paused {
            return;
        }
        if !st.running {
            st.running = true;
            drop(st);
            self.start_worker();
        }
    }

    fn start_worker(&self) {
        let me = self.clone();
        let work = move || me.work();
        if let Ok(h) = tokio::runtime::Handle::try_current() {
            drop(h.spawn_blocking(work));
            return;
        }
        let me = self.clone();
        if std::thread::Builder::new()
            .name("raft-log-flush".into())
            .spawn(work)
            .is_err()
        {
            // No thread available: flush on the caller's thread.
            me.work();
        }
    }

    fn work(&self) {
        loop {
            let jobs: Vec<Job> = {
                let mut st = lock(&self.state);
                #[cfg(test)]
                let paused = st.paused;
                #[cfg(not(test))]
                let paused = false;
                if st.queue.is_empty() || paused {
                    st.running = false;
                    return;
                }
                st.queue.drain(..).collect()
            };
            self.handle(jobs);
        }
    }

    fn handle(&self, jobs: Vec<Job>) {
        let mut files: Vec<Arc<File>> = Vec::new();
        for j in &jobs {
            if let Job::Sync { file: Some(f), .. } = j
                && !files.iter().any(|g| Arc::ptr_eq(f, g))
            {
                files.push(f.clone());
            }
        }
        let mut err = lock(&self.state).failed.clone();
        if err.is_none() {
            for f in &files {
                #[cfg(test)]
                {
                    lock(&self.state).syncs += 1;
                }
                if let Err(e) = fsutil::sync_data(f) {
                    tracing::error!("raft log sync failed: {e}");
                    let m = e.to_string();
                    lock(&self.state).failed = Some(m.clone());
                    err = Some(m);
                    break;
                }
            }
        }
        for j in jobs {
            match j {
                Job::Sync { callback, .. } => callback.log_io_completed(match &err {
                    None => Ok(()),
                    Some(m) => Err(io::Error::other(format!("log sync failed: {m}"))),
                }),
                Job::Barrier(tx) => {
                    let _ = tx.send(());
                }
            }
        }
    }
}

#[cfg(test)]
impl Flusher {
    /// `fdatasync` calls made so far.
    pub(crate) fn syncs(&self) -> u64 {
        lock(&self.state).syncs
    }

    /// While paused, queued jobs wait (neither synced nor acknowledged).
    pub(crate) fn set_paused(&self, on: bool) {
        let mut st = lock(&self.state);
        st.paused = on;
        if !on && !st.queue.is_empty() && !st.running {
            st.running = true;
            drop(st);
            self.start_worker();
        }
    }

    /// Jobs waiting (paused, or not yet taken by a worker).
    pub(crate) fn queued(&self) -> usize {
        lock(&self.state).queue.len()
    }

    /// Emulates a crash before the queued syncs: the queued jobs are
    /// dropped without syncing (their callbacks are never invoked).
    pub(crate) fn crash(&self) {
        let mut st = lock(&self.state);
        st.queue.clear();
        st.paused = true;
    }
}
