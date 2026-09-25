//! Client operation histories, as recorded by both harnesses and verified
//! by [`crate::checker`].
//!
//! Times are offsets from the start of a run on the harness's monotonic
//! clock (`tokio::time::Instant` in the in-process harness, whose time may
//! be paused; `std::time::Instant` in the multi-process one). Every
//! operation records when its command was sent and, if a reply arrived,
//! when and what. An operation without a reply ("unacknowledged") may or
//! may not have taken effect: its connection timed out or was lost, and
//! the client closed it without sending anything else.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

/// A client connection of the harness (not a server `ConnId`).
pub type ConnKey = u64;
pub type JobId = u64;
/// Index of an operation in [`History::ops`].
pub type OpId = usize;

/// The commands the workloads use (all in the `default` tube).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Cmd {
    /// `body` must be unique across the run: the checker identifies jobs
    /// by it (a `RESERVED` or `FOUND` reply names the put that created the
    /// job, even if that put was never acknowledged).
    Put {
        pri: u32,
        delay: u32,
        ttr: u32,
        body: Vec<u8>,
    },
    Reserve,
    ReserveWithTimeout(u32),
    Delete(JobId),
    Release {
        id: JobId,
        pri: u32,
        delay: u32,
    },
    Bury {
        id: JobId,
        pri: u32,
    },
    Touch(JobId),
    Kick(u32),
    KickJob(JobId),
    Peek(JobId),
    StatsJob(JobId),
}

impl Cmd {
    /// The job a command names, if any.
    pub fn target(&self) -> Option<JobId> {
        match *self {
            Cmd::Delete(id)
            | Cmd::Release { id, .. }
            | Cmd::Bury { id, .. }
            | Cmd::Touch(id)
            | Cmd::KickJob(id)
            | Cmd::Peek(id)
            | Cmd::StatsJob(id) => Some(id),
            _ => None,
        }
    }

    /// The protocol line (without the put body).
    pub fn line(&self) -> String {
        match self {
            Cmd::Put {
                pri,
                delay,
                ttr,
                body,
            } => format!("put {pri} {delay} {ttr} {}", body.len()),
            Cmd::Reserve => "reserve".into(),
            Cmd::ReserveWithTimeout(t) => format!("reserve-with-timeout {t}"),
            Cmd::Delete(id) => format!("delete {id}"),
            Cmd::Release { id, pri, delay } => format!("release {id} {pri} {delay}"),
            Cmd::Bury { id, pri } => format!("bury {id} {pri}"),
            Cmd::Touch(id) => format!("touch {id}"),
            Cmd::Kick(n) => format!("kick {n}"),
            Cmd::KickJob(id) => format!("kick-job {id}"),
            Cmd::Peek(id) => format!("peek {id}"),
            Cmd::StatsJob(id) => format!("stats-job {id}"),
        }
    }
}

/// A job's state as `stats-job` reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JobStateName {
    Ready,
    Reserved,
    Delayed,
    Buried,
}

impl JobStateName {
    pub fn parse(s: &str) -> Option<JobStateName> {
        match s {
            "ready" => Some(JobStateName::Ready),
            "reserved" => Some(JobStateName::Reserved),
            "delayed" => Some(JobStateName::Delayed),
            "buried" => Some(JobStateName::Buried),
            _ => None,
        }
    }
}

/// A reply, as far as the checker cares.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Reply {
    Inserted(JobId),
    /// `BURIED <id>`: a put that could not be enqueued.
    BuriedId(JobId),
    Reserved {
        id: JobId,
        body: Vec<u8>,
    },
    Found {
        id: JobId,
        body: Vec<u8>,
    },
    Deleted,
    Released,
    Buried,
    Touched,
    NotFound,
    Kicked(u64),
    KickedJob,
    TimedOut,
    DeadlineSoon,
    /// `OK` to `stats-job`.
    JobStats {
        id: JobId,
        state: JobStateName,
    },
    /// Anything else (the text of the reply line); always a violation.
    Other(String),
}

impl Reply {
    /// Parses a `stats-job` YAML body.
    pub fn from_stats_yaml(yaml: &[u8]) -> Reply {
        let text = String::from_utf8_lossy(yaml);
        let mut id = None;
        let mut state = None;
        for line in text.lines() {
            if let Some(v) = line.strip_prefix("id: ") {
                id = v.trim().parse().ok();
            } else if let Some(v) = line.strip_prefix("state: ") {
                state = JobStateName::parse(v.trim());
            }
        }
        match (id, state) {
            (Some(id), Some(state)) => Reply::JobStats { id, state },
            _ => Reply::Other(format!("unparsable stats-job reply: {text:?}")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpRecord {
    pub conn: ConnKey,
    pub cmd: Cmd,
    pub send: Duration,
    /// `None`: no reply (the connection was lost or timed out).
    pub reply: Option<(Duration, Reply)>,
}

impl OpRecord {
    pub fn acked(&self) -> Option<&Reply> {
        self.reply.as_ref().map(|(_, r)| r)
    }

    pub fn reply_time(&self) -> Option<Duration> {
        self.reply.as_ref().map(|(t, _)| *t)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ConnRecord {
    pub opened: Duration,
    /// When the client closed the connection or saw it closed. A
    /// connection never closed keeps its reservations (the checker never
    /// releases them by disconnect).
    pub closed: Option<Duration>,
}

/// A whole run's operations.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct History {
    pub ops: Vec<OpRecord>,
    pub conns: BTreeMap<ConnKey, ConnRecord>,
}

impl History {
    pub fn open_conn(&mut self, conn: ConnKey, at: Duration) {
        self.conns.insert(
            conn,
            ConnRecord {
                opened: at,
                closed: None,
            },
        );
    }

    pub fn close_conn(&mut self, conn: ConnKey, at: Duration) {
        if let Some(c) = self.conns.get_mut(&conn)
            && c.closed.is_none()
        {
            c.closed = Some(at);
        }
    }

    pub fn begin(&mut self, conn: ConnKey, cmd: Cmd, send: Duration) -> OpId {
        self.ops.push(OpRecord {
            conn,
            cmd,
            send,
            reply: None,
        });
        self.ops.len() - 1
    }

    pub fn finish(&mut self, op: OpId, at: Duration, reply: Reply) {
        if let Some(o) = self.ops.get_mut(op) {
            o.reply = Some((at, reply));
        }
    }

    /// A readable dump of the operations of `conns` (all if empty).
    pub fn dump(&self) -> String {
        let mut s = String::new();
        for (i, o) in self.ops.iter().enumerate() {
            s.push_str(&format!("#{i} {}\n", describe(o)));
        }
        for (c, r) in &self.conns {
            s.push_str(&format!(
                "conn {c}: opened {:?} closed {:?}\n",
                r.opened, r.closed
            ));
        }
        s
    }
}

/// One line describing an operation.
pub fn describe(o: &OpRecord) -> String {
    let reply = match &o.reply {
        None => "no reply".to_string(),
        Some((t, r)) => format!("{} at {t:?}", short_reply(r)),
    };
    let cmd = match &o.cmd {
        Cmd::Put { body, .. } => format!("{} [{}]", o.cmd.line(), String::from_utf8_lossy(body)),
        c => c.line(),
    };
    format!("conn {} sent {:?}: {cmd} -> {reply}", o.conn, o.send)
}

fn short_reply(r: &Reply) -> String {
    match r {
        Reply::Reserved { id, body } => {
            format!("RESERVED {id} [{}]", String::from_utf8_lossy(body))
        }
        Reply::Found { id, body } => format!("FOUND {id} [{}]", String::from_utf8_lossy(body)),
        other => format!("{other:?}"),
    }
}

/// A thread-safe [`History`] shared by a run's clients.
#[derive(Clone, Default)]
pub struct Recorder(Arc<Mutex<History>>);

impl Recorder {
    pub fn new() -> Recorder {
        Recorder::default()
    }

    fn lock(&self) -> MutexGuard<'_, History> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn open_conn(&self, conn: ConnKey, at: Duration) {
        self.lock().open_conn(conn, at);
    }

    pub fn close_conn(&self, conn: ConnKey, at: Duration) {
        self.lock().close_conn(conn, at);
    }

    pub fn begin(&self, conn: ConnKey, cmd: Cmd, send: Duration) -> OpId {
        self.lock().begin(conn, cmd, send)
    }

    pub fn finish(&self, op: OpId, at: Duration, reply: Reply) {
        self.lock().finish(op, at, reply);
    }

    pub fn history(&self) -> History {
        self.lock().clone()
    }
}
