//! The history checker (docs/PLAN.md §6.5).
//!
//! # Global checks
//!
//! - **Job ids are unique**: no two acknowledged puts got the same id.
//! - **Ids increase in commit order**: if put A was acknowledged before
//!   put B was sent, A's id is lower (ids are allocated in log order).
//! - **Job identity**: every put body is unique, so a `RESERVED` / `FOUND`
//!   reply names the put that created the job. The body must belong to a
//!   put that got that id, or to an unacknowledged put (which then
//!   evidently took effect, with that id). A body seen under two ids means
//!   one put created two jobs.
//! - Replies outside the protocol vocabulary of the command are
//!   violations.
//!
//! # Per-job linearizability
//!
//! Each job's operations are checked against a single-server model of that
//! job (a WGL-style depth-first search with memoization). The model's
//! states are absent, ready, reserved(conn, deadline), delayed(until),
//! buried and deleted. Every operation takes effect at one point inside
//! `[send − slack, reply + slack]`; the points follow real time and each
//! connection's program order. The point is modeled as the entry's engine
//! time, which the leader stamps after the command was sent and before the
//! reply; `slack` covers the difference between the clients' clock and
//! engine time (clock skew between nodes, process start-up anchoring).
//!
//! The search places operations at their earliest possible points
//! (`max(current point, send − slack)`): every timing constraint of the
//! model is a lower bound, so placing earlier never loses a valid
//! linearization.
//!
//! Spontaneous transitions, allowed but never required:
//! - **TTR expiry**: reserved → ready at any point at or after the
//!   reservation's (or last touch's) point + TTR (TTR 0 counts as 1 s, as
//!   in the engine);
//! - **delay expiry**: delayed → ready at or after the put's / release's
//!   point + delay;
//! - **disconnect**: reserved(C) → ready once every acknowledged operation
//!   of C (on any job) could have taken effect: at or after the send time
//!   of C's last acknowledged operation, and only if the client closed or
//!   lost C at some point. There is no upper bound: after a kill -9 of the
//!   node holding C, the release happens only when the cluster drops that
//!   node's connections, long after the client saw its connection reset;
//! - **bulk kick**: a `kick` (acknowledged with a non-zero count, or
//!   unacknowledged) may have moved a buried or delayed job to ready at a
//!   point inside its interval (it does not name its jobs).
//!
//! Unacknowledged operations are optional: each took effect at a point
//! after its send time, or never. An unacknowledged `reserve` may have
//! reserved any job; it is part of every job's history.
//!
//! When a job's search fails, the failure is classified (lost job,
//! resurrected job, exclusive holding broken, or another inconsistency)
//! and reported with the job's operations.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use crate::history::{Cmd, ConnKey, History, JobId, JobStateName, OpId, OpRecord, Reply, describe};

#[derive(Debug, Clone)]
pub struct CheckConfig {
    /// Allowed disagreement between the clients' clock and engine time.
    pub slack: Duration,
    /// Search budget per job (distinct search states); exceeding it is
    /// reported as [`ViolationKind::SearchLimit`].
    pub max_states: usize,
}

impl Default for CheckConfig {
    fn default() -> Self {
        CheckConfig {
            slack: Duration::from_millis(100),
            max_states: 2_000_000,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ViolationKind {
    /// An acknowledged `INSERTED` job is gone although no delete could have
    /// removed it.
    LostJob,
    /// An acknowledged `DELETED` job was observed again.
    Resurrected,
    /// Two acknowledged puts got the same id.
    DuplicateId,
    /// Ids are not increasing in commit (real-time) order.
    IdOrder,
    /// One put created two jobs (its body seen under two ids).
    DuplicateJob,
    /// A connection kept acting on a job another connection reserved
    /// after it.
    ExclusiveHolding,
    /// Replies not explained by any single-server execution.
    Inconsistent,
    /// A reply outside the command's vocabulary.
    UnexpectedReply,
    /// The harness broke a rule the checker relies on (duplicate bodies,
    /// operations after an unacknowledged one on the same connection).
    Harness,
    /// The per-job search exceeded its budget (not verified).
    SearchLimit,
}

#[derive(Debug, Clone)]
pub struct Violation {
    pub kind: ViolationKind,
    pub job: Option<JobId>,
    pub detail: String,
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.job {
            Some(j) => write!(f, "{:?} (job {j}): {}", self.kind, self.detail),
            None => write!(f, "{:?}: {}", self.kind, self.detail),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Report {
    pub violations: Vec<Violation>,
    /// Jobs checked (acknowledged or identified by body).
    pub jobs: usize,
    pub ops: usize,
    /// Search states visited, over all jobs.
    pub states: u64,
}

impl Report {
    pub fn ok(&self) -> bool {
        self.violations.is_empty()
    }

    pub fn summary(&self) -> String {
        let mut s = format!(
            "{} ops, {} jobs, {} search states, {} violation(s)",
            self.ops,
            self.jobs,
            self.states,
            self.violations.len()
        );
        for v in &self.violations {
            s.push_str(&format!("\n- {v}"));
        }
        s
    }
}

/// Checks `h` (see the module docs).
pub fn check(h: &History, cfg: &CheckConfig) -> Report {
    let mut rep = Report {
        ops: h.ops.len(),
        ..Report::default()
    };
    let v = &mut rep.violations;
    check_program_order(h, v);
    check_vocabulary(h, v);
    let ids = identify_jobs(h, v);
    check_id_order(h, v);
    let last_ack = last_acked_send(h);
    rep.jobs = ids.job_put.len();
    for (&job, &put) in &ids.job_put {
        rep.states += check_job(h, cfg, job, put, &last_ack, v);
    }
    rep
}

fn violation(v: &mut Vec<Violation>, kind: ViolationKind, job: Option<JobId>, detail: String) {
    v.push(Violation { kind, job, detail });
}

/// Operations after an unacknowledged one on the same connection break the
/// optional-tail assumption.
fn check_program_order(h: &History, v: &mut Vec<Violation>) {
    let mut lost: HashMap<ConnKey, OpId> = HashMap::new();
    for (i, o) in h.ops.iter().enumerate() {
        if let Some(&j) = lost.get(&o.conn) {
            violation(
                v,
                ViolationKind::Harness,
                None,
                format!("op #{i} follows unacknowledged op #{j} on conn {}", o.conn),
            );
        }
        if o.reply.is_none() {
            lost.insert(o.conn, i);
        }
    }
}

/// Whether `r` is a possible reply to `c` at all.
fn in_vocabulary(c: &Cmd, r: &Reply) -> bool {
    use Reply as R;
    match c {
        Cmd::Put { .. } => matches!(r, R::Inserted(_) | R::BuriedId(_)),
        Cmd::Reserve => matches!(r, R::Reserved { .. } | R::DeadlineSoon),
        Cmd::ReserveWithTimeout(_) => {
            matches!(r, R::Reserved { .. } | R::DeadlineSoon | R::TimedOut)
        }
        Cmd::Delete(_) => matches!(r, R::Deleted | R::NotFound),
        Cmd::Release { .. } => matches!(r, R::Released | R::NotFound),
        Cmd::Bury { .. } => matches!(r, R::Buried | R::NotFound),
        Cmd::Touch(_) => matches!(r, R::Touched | R::NotFound),
        Cmd::Kick(_) => matches!(r, R::Kicked(_)),
        Cmd::KickJob(_) => matches!(r, R::KickedJob | R::NotFound),
        Cmd::Peek(id) => match r {
            R::Found { id: f, .. } => f == id,
            R::NotFound => true,
            _ => false,
        },
        Cmd::StatsJob(id) => match r {
            R::JobStats { id: f, .. } => f == id,
            R::NotFound => true,
            _ => false,
        },
    }
}

fn check_vocabulary(h: &History, v: &mut Vec<Violation>) {
    for (i, o) in h.ops.iter().enumerate() {
        if let Some(r) = o.acked()
            && !in_vocabulary(&o.cmd, r)
        {
            violation(
                v,
                ViolationKind::UnexpectedReply,
                o.cmd.target(),
                format!("op #{i}: {}", describe(o)),
            );
        }
    }
}

struct Identities {
    /// Every known job and the put that created it.
    job_put: BTreeMap<JobId, OpId>,
}

fn put_body(o: &OpRecord) -> Option<&[u8]> {
    match &o.cmd {
        Cmd::Put { body, .. } => Some(body),
        _ => None,
    }
}

fn identify_jobs(h: &History, v: &mut Vec<Violation>) -> Identities {
    let mut body_put: HashMap<&[u8], OpId> = HashMap::new();
    for (i, o) in h.ops.iter().enumerate() {
        if let Some(b) = put_body(o)
            && let Some(prev) = body_put.insert(b, i)
        {
            violation(
                v,
                ViolationKind::Harness,
                None,
                format!("puts #{prev} and #{i} share a body"),
            );
        }
    }
    let mut job_put: BTreeMap<JobId, OpId> = BTreeMap::new();
    let mut put_job: HashMap<OpId, JobId> = HashMap::new();
    for (i, o) in h.ops.iter().enumerate() {
        if let Some(Reply::Inserted(id) | Reply::BuriedId(id)) = o.acked()
            && put_body(o).is_some()
        {
            if let Some(&prev) = job_put.get(id) {
                violation(
                    v,
                    ViolationKind::DuplicateId,
                    Some(*id),
                    format!("#{prev} {} / #{i} {}", describe(&h.ops[prev]), describe(o)),
                );
            } else {
                job_put.insert(*id, i);
                put_job.insert(i, *id);
            }
        }
    }
    for (i, o) in h.ops.iter().enumerate() {
        let (id, body) = match o.acked() {
            Some(Reply::Reserved { id, body } | Reply::Found { id, body }) => (*id, body),
            _ => continue,
        };
        let Some(&p) = body_put.get(body.as_slice()) else {
            violation(
                v,
                ViolationKind::Inconsistent,
                Some(id),
                format!("op #{i} got a body no put sent: {}", describe(o)),
            );
            continue;
        };
        match put_job.get(&p) {
            Some(&j) if j == id => {}
            Some(&j) => {
                violation(
                    v,
                    ViolationKind::DuplicateJob,
                    Some(id),
                    format!(
                        "put #{p} (job {j}) is also job {id}: #{p} {} / #{i} {}",
                        describe(&h.ops[p]),
                        describe(o)
                    ),
                );
            }
            None => {
                if let Some(&q) = job_put.get(&id) {
                    violation(
                        v,
                        ViolationKind::Inconsistent,
                        Some(id),
                        format!(
                            "job {id} of put #{q} carries the body of put #{p}: #{i} {}",
                            describe(o)
                        ),
                    );
                } else {
                    job_put.insert(id, p);
                    put_job.insert(p, id);
                }
            }
        }
    }
    // Replies about jobs no put explains.
    for (i, o) in h.ops.iter().enumerate() {
        if let (Some(t), Some(r)) = (o.cmd.target(), o.acked())
            && !job_put.contains_key(&t)
            && *r != Reply::NotFound
        {
            violation(
                v,
                ViolationKind::Inconsistent,
                Some(t),
                format!("op #{i} succeeded on a job no put created: {}", describe(o)),
            );
        }
    }
    Identities { job_put }
}

fn check_id_order(h: &History, v: &mut Vec<Violation>) {
    // (send, reply, id, op) of acknowledged puts.
    let mut puts: Vec<(Duration, Duration, JobId, OpId)> = h
        .ops
        .iter()
        .enumerate()
        .filter_map(|(i, o)| match o.reply {
            Some((t, Reply::Inserted(id) | Reply::BuriedId(id))) => Some((o.send, t, id, i)),
            _ => None,
        })
        .collect();
    let mut by_reply = puts.clone();
    by_reply.sort_by_key(|p| p.1);
    puts.sort_by_key(|p| p.0);
    let mut k = 0;
    let mut max: Option<(JobId, OpId)> = None;
    let mut reported = 0;
    for &(send, _, id, op) in &puts {
        while k < by_reply.len() && by_reply[k].1 < send {
            if max.is_none_or(|(m, _)| by_reply[k].2 > m) {
                max = Some((by_reply[k].2, by_reply[k].3));
            }
            k += 1;
        }
        if let Some((m, mop)) = max
            && m >= id
            && reported < 10
        {
            reported += 1;
            violation(
                v,
                ViolationKind::IdOrder,
                Some(id),
                format!(
                    "#{mop} {} was acknowledged before #{op} {} was sent",
                    describe(&h.ops[mop]),
                    describe(&h.ops[op])
                ),
            );
        }
    }
}

/// Send time of every connection's last acknowledged operation.
fn last_acked_send(h: &History) -> HashMap<ConnKey, Duration> {
    let mut m = HashMap::new();
    for o in &h.ops {
        if o.reply.is_some() {
            let e = m.entry(o.conn).or_insert(o.send);
            *e = (*e).max(o.send);
        }
    }
    m
}

// ---------------------------------------------------------------------------
// Per-job search
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum JobState {
    Absent,
    Ready,
    Reserved { conn: ConnKey, deadline: Duration },
    Delayed { until: Duration },
    Buried,
    Deleted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Act {
    Put { delay: u32 },
    Reserve,
    Delete,
    Release { delay: u32 },
    Bury,
    Touch,
    KickJob,
    Peek,
    StatsJob,
    Disconnect,
}

#[derive(Debug, Clone)]
struct Elem {
    conn: ConnKey,
    /// Earliest point (send − slack).
    lo: Duration,
    /// Latest point (reply + slack); `None`: unbounded.
    hi: Option<Duration>,
    required: bool,
    act: Act,
    /// The reply; `None` if unacknowledged.
    want: Option<Reply>,
}

fn secs(n: u32) -> Duration {
    Duration::from_secs(u64::from(n))
}

/// The state after `e` at point `p`, if `e` (with its reply) is possible in
/// `js`. For an unacknowledged operation: its successful effect, if it
/// could succeed (failing has no effect, the same as never happening).
fn apply(js: JobState, e: &Elem, p: Duration, ttr: Duration) -> Option<JobState> {
    use JobState as S;
    let me = e.conn;
    let mine = matches!(js, S::Reserved { conn, .. } if conn == me);
    let exists = !matches!(js, S::Absent | S::Deleted);
    let want = e.want.as_ref();
    let ok = |r: Option<&Reply>, good: Reply| r.is_none_or(|r| *r == good);
    let not_found = want == Some(&Reply::NotFound);
    match e.act {
        Act::Put { delay } => {
            if js != S::Absent {
                return None;
            }
            match want {
                Some(Reply::BuriedId(_)) => Some(S::Buried),
                None | Some(Reply::Inserted(_)) => Some(if delay > 0 {
                    S::Delayed {
                        until: p + secs(delay),
                    }
                } else {
                    S::Ready
                }),
                _ => None,
            }
        }
        Act::Reserve => match want {
            None | Some(Reply::Reserved { .. }) if js == S::Ready => Some(S::Reserved {
                conn: me,
                deadline: p + ttr,
            }),
            _ => None,
        },
        Act::Delete => {
            if not_found {
                (!exists || matches!(js, S::Reserved { .. }) && !mine).then_some(js)
            } else if ok(want, Reply::Deleted)
                && (mine || matches!(js, S::Ready | S::Delayed { .. } | S::Buried))
            {
                Some(S::Deleted)
            } else {
                None
            }
        }
        Act::Release { delay } => {
            if not_found {
                (!mine).then_some(js)
            } else if ok(want, Reply::Released) && mine {
                Some(if delay > 0 {
                    S::Delayed {
                        until: p + secs(delay),
                    }
                } else {
                    S::Ready
                })
            } else {
                None
            }
        }
        Act::Bury => {
            if not_found {
                (!mine).then_some(js)
            } else if ok(want, Reply::Buried) && mine {
                Some(S::Buried)
            } else {
                None
            }
        }
        Act::Touch => {
            if not_found {
                (!mine).then_some(js)
            } else if ok(want, Reply::Touched) && mine {
                Some(S::Reserved {
                    conn: me,
                    deadline: p + ttr,
                })
            } else {
                None
            }
        }
        Act::KickJob => {
            let kickable = matches!(js, S::Buried | S::Delayed { .. });
            if not_found {
                (!kickable).then_some(js)
            } else if ok(want, Reply::KickedJob) && kickable {
                Some(S::Ready)
            } else {
                None
            }
        }
        Act::Peek => match want {
            Some(Reply::NotFound) => (!exists).then_some(js),
            Some(Reply::Found { .. }) => exists.then_some(js),
            _ => None,
        },
        Act::StatsJob => match want {
            Some(Reply::NotFound) => (!exists).then_some(js),
            Some(Reply::JobStats { state, .. }) => {
                let actual = match js {
                    S::Ready => JobStateName::Ready,
                    S::Reserved { .. } => JobStateName::Reserved,
                    S::Delayed { .. } => JobStateName::Delayed,
                    S::Buried => JobStateName::Buried,
                    S::Absent | S::Deleted => return None,
                };
                (actual == *state).then_some(js)
            }
            _ => None,
        },
        Act::Disconnect => None,
    }
}

/// The holder of a job reserved by an anonymous lost `reserve` (see
/// [`Search::phantoms`]).
const PHANTOM: ConnKey = ConnKey::MAX;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Key {
    cur: Vec<u16>,
    kicks: Vec<bool>,
    phantoms: u16,
    js: JobState,
    lo: Duration,
}

struct Search {
    conns: Vec<Vec<Elem>>,
    /// Bulk kicks: (lo, hi).
    kicks: Vec<(Duration, Option<Duration>)>,
    /// Earliest points of the unacknowledged `reserve`s of closed
    /// connections that have nothing else to do with this job, ascending.
    /// Such a reserve may have held the job over any interval starting
    /// after its send (its connection's disconnect can release it at once:
    /// its last acknowledged operation was sent before it), so one sent
    /// earlier can do everything one sent later can: they are used in
    /// order, and the search state only counts how many are used.
    phantoms: Vec<Duration>,
    ttr: Duration,
    failed: HashSet<Key>,
    max_states: usize,
    states: u64,
    limited: bool,
}

/// The mutable part of a search state.
struct Pos {
    cur: Vec<u16>,
    kicks: Vec<bool>,
    phantoms: u16,
}

impl Search {
    fn dfs(&mut self, pos: &mut Pos, js: JobState, lo: Duration) -> bool {
        if self.limited {
            return false;
        }
        // Every remaining required element must fit after the next point.
        let mut bound: Option<Duration> = None;
        let mut remaining = false;
        for (ci, c) in self.conns.iter().enumerate() {
            if let Some(e) = c.get(pos.cur[ci] as usize)
                && e.required
            {
                remaining = true;
                if let Some(h) = e.hi {
                    bound = Some(bound.map_or(h, |b| b.min(h)));
                }
            }
        }
        if !remaining {
            return true;
        }
        let fits = |p: Duration| bound.is_none_or(|b| p <= b);
        let key = Key {
            cur: pos.cur.clone(),
            kicks: pos.kicks.clone(),
            phantoms: pos.phantoms,
            js,
            lo,
        };
        if self.failed.contains(&key) {
            return false;
        }
        self.states += 1;
        if self.failed.len() >= self.max_states {
            self.limited = true;
            return false;
        }

        // Operations at the connections' cursors: required ones first.
        for pass_required in [true, false] {
            for ci in 0..self.conns.len() {
                let i = pos.cur[ci] as usize;
                let Some(e) = self.conns[ci].get(i) else {
                    continue;
                };
                if e.required != pass_required {
                    continue;
                }
                let e = e.clone();
                let holder = matches!(js, JobState::Reserved { conn, .. } if conn == e.conn);
                if !e.required && holder {
                    // The holder's disconnect: its required operations are
                    // all placed (an unacknowledged tail is abandoned).
                    if let Some(d) = self.conns[ci].last()
                        && d.act == Act::Disconnect
                    {
                        let p = lo.max(d.lo);
                        if fits(p) {
                            let saved = pos.cur[ci];
                            pos.cur[ci] = self.conns[ci].len() as u16;
                            if self.dfs(pos, JobState::Ready, p) {
                                return true;
                            }
                            pos.cur[ci] = saved;
                        }
                    }
                }
                if e.act == Act::Disconnect {
                    continue;
                }
                let p = lo.max(e.lo);
                if !fits(p) || e.hi.is_some_and(|h| p > h) {
                    continue;
                }
                if let Some(ns) = apply(js, &e, p, self.ttr) {
                    pos.cur[ci] += 1;
                    let ok = self.dfs(pos, ns, p);
                    pos.cur[ci] -= 1;
                    if ok {
                        return true;
                    }
                }
            }
        }

        // Spontaneous transitions.
        match js {
            JobState::Reserved { conn: PHANTOM, .. } => {
                if self.dfs(pos, JobState::Ready, lo) {
                    return true;
                }
            }
            JobState::Reserved { deadline: t, .. } | JobState::Delayed { until: t } => {
                let p = lo.max(t);
                if fits(p) && self.dfs(pos, JobState::Ready, p) {
                    return true;
                }
            }
            _ => {}
        }
        if matches!(js, JobState::Buried | JobState::Delayed { .. }) {
            for k in 0..self.kicks.len() {
                if pos.kicks[k] {
                    continue;
                }
                let (klo, khi) = self.kicks[k];
                let p = lo.max(klo);
                if fits(p) && khi.is_none_or(|h| p <= h) {
                    pos.kicks[k] = true;
                    let ok = self.dfs(pos, JobState::Ready, p);
                    pos.kicks[k] = false;
                    if ok {
                        return true;
                    }
                }
            }
        }
        if js == JobState::Ready
            && let Some(&plo) = self.phantoms.get(pos.phantoms as usize)
        {
            let p = lo.max(plo);
            if fits(p) {
                pos.phantoms += 1;
                let held = JobState::Reserved {
                    conn: PHANTOM,
                    deadline: p + self.ttr,
                };
                let ok = self.dfs(pos, held, p);
                pos.phantoms -= 1;
                if ok {
                    return true;
                }
            }
        }
        self.failed.insert(key);
        false
    }
}

/// Checks one job (reporting a failure to `v`); returns the states
/// visited.
fn check_job(
    h: &History,
    cfg: &CheckConfig,
    job: JobId,
    put: OpId,
    last_ack: &HashMap<ConnKey, Duration>,
    v: &mut Vec<Violation>,
) -> u64 {
    let slack = cfg.slack;
    let (delay, ttr) = match &h.ops[put].cmd {
        Cmd::Put { delay, ttr, .. } => (*delay, secs((*ttr).max(1))),
        _ => return 0,
    };
    let mut per_conn: BTreeMap<ConnKey, Vec<(OpId, Elem)>> = BTreeMap::new();
    let mut kicks = Vec::new();
    let mut last_required: Duration = Duration::ZERO;
    for (i, o) in h.ops.iter().enumerate() {
        let act = match &o.cmd {
            Cmd::Put { .. } if i == put => Act::Put { delay },
            Cmd::Put { .. } => continue,
            Cmd::Reserve | Cmd::ReserveWithTimeout(_) => match o.acked() {
                Some(Reply::Reserved { id, .. }) if *id == job => Act::Reserve,
                None => Act::Reserve,
                _ => continue,
            },
            Cmd::Kick(_) => {
                if !matches!(o.acked(), Some(Reply::Kicked(0))) {
                    kicks.push((
                        o.send.saturating_sub(slack),
                        o.reply_time().map(|t| t + slack),
                    ));
                }
                continue;
            }
            c if c.target() != Some(job) => continue,
            Cmd::Delete(_) => Act::Delete,
            Cmd::Release { delay, .. } => Act::Release { delay: *delay },
            Cmd::Bury { .. } => Act::Bury,
            Cmd::Touch(_) => Act::Touch,
            Cmd::KickJob(_) => Act::KickJob,
            Cmd::Peek(_) => Act::Peek,
            Cmd::StatsJob(_) => Act::StatsJob,
        };
        let required = o.reply.is_some() || i == put;
        if !required && matches!(act, Act::Peek | Act::StatsJob) {
            // No effect either way.
            continue;
        }
        let hi = o.reply_time().map(|t| t + slack);
        if required && let Some(t) = hi {
            last_required = last_required.max(t);
        }
        per_conn.entry(o.conn).or_default().push((
            i,
            Elem {
                conn: o.conn,
                lo: o.send.saturating_sub(slack),
                hi,
                required,
                act,
                want: o.acked().cloned(),
            },
        ));
    }
    // Optional operations sent after every required one ends cannot matter.
    for list in per_conn.values_mut() {
        list.retain(|(_, e)| e.required || e.lo <= last_required);
    }
    per_conn.retain(|_, l| !l.is_empty());
    let mut conns = Vec::new();
    let mut index = Vec::new();
    let mut phantoms = Vec::new();
    for (c, list) in per_conn {
        let closed = h.conns.get(&c).is_some_and(|r| r.closed.is_some());
        if let [(i, e)] = list.as_slice()
            && closed
            && e.act == Act::Reserve
            && !e.required
        {
            index.push((c, *i));
            phantoms.push(e.lo);
            continue;
        }
        let mut elems: Vec<Elem> = Vec::new();
        for (i, e) in list {
            index.push((c, i));
            elems.push(e);
        }
        if let Some(rec) = h.conns.get(&c)
            && rec.closed.is_some()
        {
            let lo = last_ack.get(&c).copied().unwrap_or(rec.opened);
            elems.push(Elem {
                conn: c,
                lo: lo.saturating_sub(slack),
                hi: None,
                required: false,
                act: Act::Disconnect,
                want: None,
            });
        }
        conns.push(elems);
    }
    phantoms.sort_unstable();
    let n = conns.len();
    let mut s = Search {
        conns,
        kicks: kicks.clone(),
        phantoms,
        ttr,
        failed: HashSet::new(),
        max_states: cfg.max_states,
        states: 0,
        limited: false,
    };
    let mut pos = Pos {
        cur: vec![0u16; n],
        kicks: vec![false; kicks.len()],
        phantoms: 0,
    };
    let ok = s.dfs(&mut pos, JobState::Absent, Duration::ZERO);
    if ok {
        return s.states;
    }
    let ops: Vec<OpId> = {
        let mut o: Vec<OpId> = index.iter().map(|&(_, i)| i).collect();
        o.sort_unstable();
        o
    };
    let mut detail = String::new();
    for &i in &ops {
        detail.push_str(&format!("\n    #{i} {}", describe(&h.ops[i])));
    }
    if !kicks.is_empty() {
        detail.push_str(&format!("\n    ({} bulk kick(s))", kicks.len()));
    }
    let kind = if s.limited {
        ViolationKind::SearchLimit
    } else {
        classify(h, job, &ops)
    };
    violation(
        v,
        kind,
        Some(job),
        format!("no valid linearization:{detail}"),
    );
    s.states
}

/// Why a job's history has no linearization (a best guess for the report).
fn classify(h: &History, job: JobId, ops: &[OpId]) -> ViolationKind {
    let ops: Vec<&OpRecord> = ops.iter().map(|&i| &h.ops[i]).collect();
    let observed = |o: &OpRecord| {
        matches!(
            o.acked(),
            Some(
                Reply::Found { .. }
                    | Reply::Reserved { .. }
                    | Reply::JobStats { .. }
                    | Reply::Touched
                    | Reply::Released
                    | Reply::Buried
                    | Reply::KickedJob
                    | Reply::Deleted
            )
        )
    };
    // Acknowledged delete, then the job seen again.
    for d in &ops {
        if d.cmd == Cmd::Delete(job)
            && let Some((t, Reply::Deleted)) = &d.reply
            && ops.iter().any(|o| o.send > *t && observed(o))
        {
            return ViolationKind::Resurrected;
        }
    }
    // Two holders: A reserved, B reserved later, then A acted as holder.
    for a in &ops {
        let Some((ta, Reply::Reserved { .. })) = &a.reply else {
            continue;
        };
        for b in &ops {
            let Some((tb, Reply::Reserved { .. })) = &b.reply else {
                continue;
            };
            if b.conn == a.conn || b.send < *ta {
                continue;
            }
            if ops.iter().any(|x| {
                x.conn == a.conn
                    && x.send > *tb
                    && matches!(
                        x.acked(),
                        Some(Reply::Deleted | Reply::Released | Reply::Buried | Reply::Touched)
                    )
            }) {
                return ViolationKind::ExclusiveHolding;
            }
        }
    }
    // Gone without any delete that could explain it.
    let any_delete = ops
        .iter()
        .any(|o| o.cmd == Cmd::Delete(job) && o.acked() != Some(&Reply::NotFound));
    let missing = ops.iter().any(|o| {
        matches!(o.cmd, Cmd::Peek(_) | Cmd::StatsJob(_)) && o.acked() == Some(&Reply::NotFound)
    });
    if missing && !any_delete {
        return ViolationKind::LostJob;
    }
    ViolationKind::Inconsistent
}

#[cfg(test)]
mod tests;
