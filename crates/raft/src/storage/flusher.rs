//! Group commit for the log store (P3-FD; docs/DESIGN.md §8 "Group commit"):
//! `append` writes its records and queues its openraft callback here; a flush
//! worker `fdatasync`s every file written since its last sync once, then
//! invokes all the callbacks it covered, in queue order (= append order).
//!
//! At most one worker runs. It starts when a job is queued and none is
//! running, takes every queued job, syncs the distinct files they name
//! (usually just the last segment), invokes the callbacks, and repeats until
//! the queue is empty; jobs queued during a sync share the next one. It runs
//! on tokio's blocking pool (or a new thread outside a runtime), never on the
//! caller's task. Being a blocking task also keeps a paused test clock from
//! jumping ahead during a sync (see the chaos harness).
//!
//! Invariants: a job is queued after its records were written (under the log
//! store's lock) and the worker syncs its file after taking it, so a callback
//! runs only after its entries are durable. A failed sync fails every callback
//! it covered and every later append (sticky): the state of the written pages
//! is then unknown. [`Flusher::barrier`] waits until every job queued before
//! it is done; `truncate`, `purge` and `save_vote` call it first, so nothing
//! is acknowledged after it was cut off and a truncated or deleted segment is
//! never synced afterwards.

use std::collections::VecDeque;
use std::fs::File;
use std::io;
use std::sync::{Arc, Mutex, MutexGuard};

use openraft::storage::LogFlushed;
use tokio::sync::oneshot;

use super::fsutil;
use crate::TypeConfig;

enum Job {
    Sync {
        file: Option<Arc<File>>,
        callback: LogFlushed<TypeConfig>,
    },
    Barrier(oneshot::Sender<()>),
}

#[derive(Default)]
struct State {
    queue: VecDeque<Job>,
    running: bool,
    failed: Option<String>,
    /// `fdatasync` calls made (tests).
    #[cfg(test)]
    syncs: u64,
    #[cfg(test)]
    paused: bool,
}

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
    pub(crate) fn syncs(&self) -> u64 {
        lock(&self.state).syncs
    }

    pub(crate) fn set_paused(&self, on: bool) {
        let mut st = lock(&self.state);
        st.paused = on;
        if !on && !st.queue.is_empty() && !st.running {
            st.running = true;
            drop(st);
            self.start_worker();
        }
    }

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
