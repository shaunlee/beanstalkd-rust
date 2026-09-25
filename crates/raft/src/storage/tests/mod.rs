#![allow(clippy::unwrap_used)]

mod log;
mod sm;
mod suite;

use std::future::Future;
use std::path::Path;
use std::sync::{Arc, Mutex};

use bstk_engine::{ConnId, EngineConfig, EngineInput, StaticSysInfo, SysInfo};
use bstk_proto::Response;
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};

use super::{ClusterStateMachine, LogOptions, LogStore, ReplySink, SmOptions};
use crate::{NodeId, Op, Request, TypeConfig};

/// Run a future on a fresh single-threaded runtime (keeps the thread-local
/// crash points on this thread).
pub(super) fn block_on<F: Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(f)
}

pub(super) fn lid(term: u64, index: u64) -> LogId<NodeId> {
    LogId::new(CommittedLeaderId::new(term, 1), index)
}

pub(super) fn normal(term: u64, index: u64, req: Request) -> Entry<TypeConfig> {
    Entry {
        log_id: lid(term, index),
        payload: EntryPayload::Normal(req),
    }
}

pub(super) fn blank(term: u64, index: u64) -> Entry<TypeConfig> {
    Entry {
        log_id: lid(term, index),
        payload: EntryPayload::Blank,
    }
}

/// A log entry whose size varies with `index`.
pub(super) fn filler(term: u64, index: u64) -> Entry<TypeConfig> {
    let body = vec![b'x'; (index as usize * 7) % 50];
    normal(
        term,
        index,
        Request {
            now: index,
            op: Op::Conn {
                seq: index,
                input: EngineInput::Command {
                    conn: 1,
                    cmd: bstk_proto::Command::Put {
                        pri: 1,
                        delay: 0,
                        ttr: 1,
                        body: body.into(),
                    },
                },
            },
        },
    )
}

pub(super) fn small_log_opts() -> LogOptions {
    LogOptions {
        segment_size: 400,
        max_read_bytes: 1 << 20,
    }
}

pub(super) fn open_log(dir: &Path) -> LogStore {
    LogStore::open(dir, small_log_opts()).unwrap()
}

/// What a sink saw.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Ev {
    Applied(ConnId, u64),
    Deliver(ConnId, Response),
    Closed(ConnId),
}

#[derive(Default)]
pub(super) struct RecSink(pub Mutex<Vec<Ev>>);

impl RecSink {
    pub(super) fn take(&self) -> Vec<Ev> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}

impl ReplySink for RecSink {
    fn applied(&self, conn: ConnId, seq: u64) {
        self.0.lock().unwrap().push(Ev::Applied(conn, seq));
    }
    fn deliver(&self, conn: ConnId, resp: Response) {
        self.0.lock().unwrap().push(Ev::Deliver(conn, resp));
    }
    fn closed(&self, conn: ConnId) {
        self.0.lock().unwrap().push(Ev::Closed(conn));
    }
}

pub(super) fn sys() -> Box<dyn SysInfo> {
    Box::new(StaticSysInfo::default())
}

pub(super) fn sm_opts(node: NodeId, sink: Arc<RecSink>) -> SmOptions {
    SmOptions {
        node_id: node,
        engine: EngineConfig::default(),
        sys: Arc::new(sys),
        sink,
    }
}

pub(super) fn open_sm(dir: &Path, node: NodeId, sink: Arc<RecSink>) -> ClusterStateMachine {
    ClusterStateMachine::open(dir, sm_opts(node, sink)).unwrap()
}
