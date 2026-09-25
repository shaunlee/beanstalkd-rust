//! The deterministic beanstalkd state machine. Ground truth for every rule
//! implemented here is `.ref/beanstalkd/{prot,conn,tube,job,ms,heap}.c`.

use std::collections::HashMap;

use bytes::Bytes;

use bstk_proto::{
    Command, JobId, PutRejection, Response, StatsJob, StatsServer, StatsTube, TubeName,
    URGENT_THRESHOLD,
};

use crate::model::{ConnState, JobRec, JobState, TubeState};
use crate::ms::Ms;
use crate::{ConnId, EngineConfig, NANOS_PER_SEC, Nanos, Outbox, SysInfo};

/// `SAFETY_MARGIN` in conn.c: 1 second.
const SAFETY_MARGIN: Nanos = NANOS_PER_SEC;

pub struct Engine {
    cfg: EngineConfig,
    sys: Box<dyn SysInfo>,
    start: Nanos,
    draining: bool,

    next_job_id: JobId,
    jobs: HashMap<JobId, JobRec>,

    tubes: HashMap<TubeName, TubeState>,
    /// Mirrors the reference's global `tubes` `Ms` array: insertion order,
    /// with swap-removal on GC. Drives list-tubes output order.
    tube_order: Ms<TubeName>,

    conns: HashMap<ConnId, ConnState>,

    // Global counters (see `struct stats global_stat` and friends in dat.h).
    ready_ct: u64,
    urgent_ct: u64,
    reserved_ct: u64,
    buried_ct: u64,
    waiting_ct: u64,
    total_jobs_ct: u64,
    timeout_ct: u64,

    cur_conns: u32,
    tot_conns: u32,
    cur_producers: u32,
    cur_workers: u32,

    // op_ct[] -- one counter per command that appears in STATS_FMT. Commands
    // not reported by the reference (reserve-job, kick-job) are not tracked.
    cmd_put: u64,
    cmd_peek: u64,
    cmd_peek_ready: u64,
    cmd_peek_delayed: u64,
    cmd_peek_buried: u64,
    cmd_reserve: u64,
    cmd_reserve_with_timeout: u64,
    cmd_delete: u64,
    cmd_release: u64,
    cmd_use: u64,
    cmd_watch: u64,
    cmd_ignore: u64,
    cmd_bury: u64,
    cmd_kick: u64,
    cmd_touch: u64,
    cmd_stats: u64,
    cmd_stats_job: u64,
    cmd_stats_tube: u64,
    cmd_list_tubes: u64,
    cmd_list_tube_used: u64,
    cmd_list_tubes_watched: u64,
    cmd_pause_tube: u64,
}

impl Engine {
    pub fn new(now: Nanos, cfg: EngineConfig, sys: Box<dyn SysInfo>) -> Self {
        let mut e = Engine {
            cfg,
            sys,
            start: now,
            draining: false,
            next_job_id: 1,
            jobs: HashMap::new(),
            tubes: HashMap::new(),
            tube_order: Ms::new(),
            conns: HashMap::new(),
            ready_ct: 0,
            urgent_ct: 0,
            reserved_ct: 0,
            buried_ct: 0,
            waiting_ct: 0,
            total_jobs_ct: 0,
            timeout_ct: 0,
            cur_conns: 0,
            tot_conns: 0,
            cur_producers: 0,
            cur_workers: 0,
            cmd_put: 0,
            cmd_peek: 0,
            cmd_peek_ready: 0,
            cmd_peek_delayed: 0,
            cmd_peek_buried: 0,
            cmd_reserve: 0,
            cmd_reserve_with_timeout: 0,
            cmd_delete: 0,
            cmd_release: 0,
            cmd_use: 0,
            cmd_watch: 0,
            cmd_ignore: 0,
            cmd_bury: 0,
            cmd_kick: 0,
            cmd_touch: 0,
            cmd_stats: 0,
            cmd_stats_job: 0,
            cmd_stats_tube: 0,
            cmd_list_tubes: 0,
            cmd_list_tube_used: 0,
            cmd_list_tubes_watched: 0,
            cmd_pause_tube: 0,
        };
        // The "default" tube is immortal (see TubeState::refs / gc_tube_if_orphan).
        e.find_or_make_tube(&TubeName::default_tube());
        e
    }

    pub fn connect(&mut self, _now: Nanos, conn: ConnId) {
        let default = TubeName::default_tube();
        self.find_or_make_tube(&default);
        let t = self.tubes.get_mut(&default).expect("default tube exists");
        t.using_ct += 1;
        t.watching_ct += 1;
        self.conns.insert(conn, ConnState::new(default));
        self.cur_conns += 1;
        self.tot_conns += 1;
    }

    pub fn disconnect(&mut self, now: Nanos, conn: ConnId, out: &mut Outbox) {
        if !self.conns.contains_key(&conn) {
            return;
        }
        self.do_remove_waiting_conn(conn);

        // Release reserved jobs, oldest reservation first, running
        // process_queue after each one (see enqueue_reserved_jobs in
        // prot.c, called from connclose).
        let reserved: Vec<JobId> = self
            .conns
            .get(&conn)
            .map(|c| c.reserved_fifo.clone())
            .unwrap_or_default();
        for job_id in reserved {
            self.do_unreserve(conn, job_id);
            if let Some(j) = self.jobs.get(&job_id) {
                let tube = j.tube.clone();
                self.insert_ready(&tube, job_id);
            }
            self.process_queue(now, out);
        }

        if let Some(c) = self.conns.get(&conn) {
            let watched: Vec<TubeName> = c.watch.items.clone();
            for t in watched {
                if let Some(ts) = self.tubes.get_mut(&t) {
                    ts.watching_ct = ts.watching_ct.saturating_sub(1);
                }
                self.gc_tube_if_orphan(&t);
            }
        }

        if let Some(c) = self.conns.get(&conn) {
            let use_tube = c.use_tube.clone();
            if let Some(ts) = self.tubes.get_mut(&use_tube) {
                ts.using_ct = ts.using_ct.saturating_sub(1);
            }
            self.gc_tube_if_orphan(&use_tube);
        }

        if let Some(c) = self.conns.remove(&conn) {
            if c.is_producer {
                self.cur_producers = self.cur_producers.saturating_sub(1);
            }
            if c.is_worker {
                self.cur_workers = self.cur_workers.saturating_sub(1);
            }
        }
        self.cur_conns = self.cur_conns.saturating_sub(1);
    }

    pub fn half_close(&mut self, _now: Nanos, conn: ConnId, out: &mut Outbox) {
        let waiting = self.conns.get(&conn).map(|c| c.waiting).unwrap_or(false);
        if waiting {
            self.do_remove_waiting_conn(conn);
            out.push((conn, Response::TimedOut));
        }
    }

    /// A `put` rejected by the codec. Mirrors the side effects prot.c
    /// performs before each rejection: `op_ct[OP_PUT]++` happens right after
    /// the numeric fields parse, and the EXPECTED_CRLF path has already run
    /// `connsetproducer` and `make_job` (which consumes a job id).
    pub fn put_rejected(&mut self, _now: Nanos, conn: ConnId, why: PutRejection, out: &mut Outbox) {
        if !self.conns.contains_key(&conn) {
            return;
        }
        self.cmd_put += 1;
        if why == PutRejection::ExpectedCrlf {
            self.connsetproducer(conn);
            self.next_job_id += 1;
        }
        out.push((conn, why.response()));
    }

    pub fn handle(&mut self, now: Nanos, conn: ConnId, cmd: Command, out: &mut Outbox) {
        if !self.conns.contains_key(&conn) {
            return;
        }
        match cmd {
            Command::Put {
                pri,
                delay,
                ttr,
                body,
            } => self.cmd_put(now, conn, pri, delay, ttr, body, out),
            Command::Use(tube) => self.cmd_use(conn, tube, out),
            Command::Reserve => self.cmd_reserve(now, conn, None, out),
            Command::ReserveWithTimeout(t) => self.cmd_reserve(now, conn, Some(t), out),
            Command::ReserveJob(id) => self.cmd_reserve_job(now, conn, id, out),
            Command::Delete(id) => self.cmd_delete(conn, id, out),
            Command::Release { id, pri, delay } => self.cmd_release(now, conn, id, pri, delay, out),
            Command::Bury { id, pri } => self.cmd_bury(conn, id, pri, out),
            Command::Touch(id) => self.cmd_touch(now, conn, id, out),
            Command::Watch(tube) => self.cmd_watch(conn, tube, out),
            Command::Ignore(tube) => self.cmd_ignore(conn, tube, out),
            Command::Peek(id) => self.cmd_peek(conn, id, out),
            Command::PeekReady => self.cmd_peek_ready(conn, out),
            Command::PeekDelayed => self.cmd_peek_delayed(conn, out),
            Command::PeekBuried => self.cmd_peek_buried(conn, out),
            Command::Kick(n) => self.cmd_kick(now, conn, n, out),
            Command::KickJob(id) => self.cmd_kick_job(now, conn, id, out),
            Command::StatsJob(id) => self.cmd_stats_job(now, conn, id, out),
            Command::StatsTube(tube) => self.cmd_stats_tube(now, conn, tube, out),
            Command::Stats => self.cmd_stats(now, conn, out),
            Command::ListTubes => self.cmd_list_tubes(conn, out),
            Command::ListTubeUsed => self.cmd_list_tube_used(conn, out),
            Command::ListTubesWatched => self.cmd_list_tubes_watched(conn, out),
            // The server must never forward Quit to the engine.
            Command::Quit => {}
            Command::PauseTube { tube, delay } => self.cmd_pause_tube(now, conn, tube, delay, out),
        }
    }

    pub fn tick(&mut self, now: Nanos, out: &mut Outbox) {
        // 1. Delayed jobs whose deadline has passed.
        loop {
            let due = self
                .soonest_delayed_job()
                .filter(|&(deadline, _, _)| deadline <= now);
            let Some((_, tube, id)) = due else { break };
            self.remove_delayed(&tube, id);
            self.insert_ready(&tube, id);
            self.process_queue(now, out);
        }

        // 2. Tube pauses whose expiry has passed, in tube_order order.
        let names: Vec<TubeName> = self.tube_order.items.clone();
        for name in names {
            let due = self
                .tubes
                .get(&name)
                .map(|t| t.pause > 0 && t.unpause_at <= now)
                .unwrap_or(false);
            if due {
                if let Some(t) = self.tubes.get_mut(&name) {
                    t.pause = 0;
                }
                self.process_queue(now, out);
            }
        }

        // 3. Connections with a due TTR/margin/explicit-timeout event,
        // processed one at a time (recomputing the earliest each round,
        // since processing one connection can change others' schedules via
        // process_queue reassignment). Each connection is handled at most
        // once per tick() call: `conn_timeout` drains every one of *its*
        // overdue reserved jobs and reaches a final, stable decision, so a
        // second pass over the same still-due connection (e.g. an exact
        // boundary case where the deadline equals `now`, mirroring the
        // reference's strict `>=` check, which yields a genuine no-op)
        // cannot make further progress and must not be retried.
        let mut processed: std::collections::HashSet<ConnId> = std::collections::HashSet::new();
        loop {
            let mut best: Option<(Nanos, ConnId)> = None;
            for &cid in self.conns.keys() {
                if processed.contains(&cid) {
                    continue;
                }
                if let Some(t) = self.conn_tickat(cid) {
                    best = Some(match best {
                        None => (t, cid),
                        Some((bt, bid)) => {
                            if (t, cid) < (bt, bid) {
                                (t, cid)
                            } else {
                                (bt, bid)
                            }
                        }
                    });
                }
            }
            match best {
                Some((t, cid)) if t <= now => {
                    processed.insert(cid);
                    self.conn_timeout(cid, now, out);
                }
                _ => break,
            }
        }
    }

    pub fn next_deadline(&self) -> Option<Nanos> {
        let mut best: Option<Nanos> = None;
        if let Some((d, _, _)) = self.soonest_delayed_job() {
            best = Some(best.map_or(d, |b| b.min(d)));
        }
        for t in self.tubes.values() {
            if t.pause > 0 {
                best = Some(best.map_or(t.unpause_at, |b| b.min(t.unpause_at)));
            }
        }
        for &cid in self.conns.keys() {
            if let Some(t) = self.conn_tickat(cid) {
                best = Some(best.map_or(t, |b| b.min(t)));
            }
        }
        best
    }

    pub fn set_draining(&mut self, on: bool) {
        self.draining = on;
    }
}

// ---------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------
impl Engine {
    fn find_or_make_tube(&mut self, name: &TubeName) {
        if !self.tubes.contains_key(name) {
            self.tubes
                .insert(name.clone(), TubeState::new(name.clone()));
            self.tube_order.append(name.clone());
        }
    }

    /// Destroys a tube if it has no more uses, watchers or jobs. "default"
    /// is immortal (mirrors the permanent reference held by the reference
    /// implementation's static `default_tube` pointer).
    fn gc_tube_if_orphan(&mut self, name: &TubeName) {
        if name.as_str() == "default" {
            return;
        }
        let orphan = self.tubes.get(name).map(|t| t.refs() == 0).unwrap_or(false);
        if orphan {
            self.tubes.remove(name);
            self.tube_order.remove(name);
        }
    }

    fn insert_ready(&mut self, tube: &TubeName, id: JobId) {
        let pri = match self.jobs.get_mut(&id) {
            Some(j) => {
                j.state = JobState::Ready;
                j.reserver = None;
                j.pri
            }
            None => return,
        };
        if let Some(t) = self.tubes.get_mut(tube) {
            t.ready.insert((pri, id));
        }
        self.ready_ct += 1;
        if pri < URGENT_THRESHOLD {
            self.urgent_ct += 1;
            if let Some(t) = self.tubes.get_mut(tube) {
                t.stat.urgent_ct += 1;
            }
        }
    }

    fn remove_ready(&mut self, tube: &TubeName, id: JobId) {
        let pri = self.jobs.get(&id).map(|j| j.pri).unwrap_or(0);
        if let Some(t) = self.tubes.get_mut(tube) {
            t.ready.remove(&(pri, id));
        }
        self.ready_ct = self.ready_ct.saturating_sub(1);
        if pri < URGENT_THRESHOLD {
            self.urgent_ct = self.urgent_ct.saturating_sub(1);
            if let Some(t) = self.tubes.get_mut(tube) {
                t.stat.urgent_ct = t.stat.urgent_ct.saturating_sub(1);
            }
        }
    }

    fn insert_delayed(&mut self, tube: &TubeName, id: JobId, deadline: Nanos) {
        if let Some(j) = self.jobs.get_mut(&id) {
            j.state = JobState::Delayed;
            j.reserver = None;
            j.deadline_at = deadline;
        }
        if let Some(t) = self.tubes.get_mut(tube) {
            t.delayed.insert((deadline, id));
        }
    }

    fn remove_delayed(&mut self, tube: &TubeName, id: JobId) {
        let deadline = self.jobs.get(&id).map(|j| j.deadline_at).unwrap_or(0);
        if let Some(t) = self.tubes.get_mut(tube) {
            t.delayed.remove(&(deadline, id));
        }
    }

    fn insert_buried(&mut self, tube: &TubeName, id: JobId) {
        if let Some(j) = self.jobs.get_mut(&id) {
            j.state = JobState::Buried;
            j.reserver = None;
            j.bury_ct += 1;
        }
        if let Some(t) = self.tubes.get_mut(tube) {
            t.buried.push_back(id);
            t.stat.buried_ct += 1;
        }
        self.buried_ct += 1;
    }

    /// Removes a specific job from the buried FIFO (used by delete and
    /// kick-job, which can target any buried job, not just the front).
    fn remove_buried(&mut self, tube: &TubeName, id: JobId) -> bool {
        let removed = match self.tubes.get_mut(tube) {
            Some(t) => match t.buried.iter().position(|&x| x == id) {
                Some(pos) => {
                    t.buried.remove(pos);
                    t.stat.buried_ct = t.stat.buried_ct.saturating_sub(1);
                    true
                }
                None => false,
            },
            None => false,
        };
        if removed {
            self.buried_ct = self.buried_ct.saturating_sub(1);
        }
        removed
    }

    fn pop_buried_front(&mut self, tube: &TubeName) -> Option<JobId> {
        let id = self.tubes.get_mut(tube).and_then(|t| t.buried.pop_front());
        if id.is_some() {
            if let Some(t) = self.tubes.get_mut(tube) {
                t.stat.buried_ct = t.stat.buried_ct.saturating_sub(1);
            }
            self.buried_ct = self.buried_ct.saturating_sub(1);
        }
        id
    }

    /// `conn_reserve_job`: assigns `job_id` to `cid`, sets its TTR
    /// deadline, and updates every reserved-job counter.
    fn do_reserve(&mut self, cid: ConnId, job_id: JobId, now: Nanos) {
        let (tube, ttr) = match self.jobs.get(&job_id) {
            Some(j) => (j.tube.clone(), j.ttr),
            None => return,
        };
        let deadline = now + (ttr as Nanos) * NANOS_PER_SEC;
        if let Some(j) = self.jobs.get_mut(&job_id) {
            j.state = JobState::Reserved;
            j.reserver = Some(cid);
            j.deadline_at = deadline;
            j.reserve_ct += 1;
        }
        if let Some(c) = self.conns.get_mut(&cid) {
            c.reserved_fifo.push(job_id);
            c.reserved_by_deadline.insert((deadline, job_id));
        }
        self.reserved_ct += 1;
        if let Some(t) = self.tubes.get_mut(&tube) {
            t.stat.reserved_ct += 1;
        }
    }

    /// Removes `job_id` from `cid`'s reservation bookkeeping and decrements
    /// the reserved-job counters. Does not change the job's state; the
    /// caller decides what happens to the job next.
    fn do_unreserve(&mut self, cid: ConnId, job_id: JobId) {
        let deadline = self.jobs.get(&job_id).map(|j| j.deadline_at).unwrap_or(0);
        let tube = self.jobs.get(&job_id).map(|j| j.tube.clone());
        if let Some(c) = self.conns.get_mut(&cid) {
            c.reserved_fifo.retain(|&x| x != job_id);
            c.reserved_by_deadline.remove(&(deadline, job_id));
        }
        if let Some(j) = self.jobs.get_mut(&job_id) {
            j.reserver = None;
        }
        self.reserved_ct = self.reserved_ct.saturating_sub(1);
        if let Some(t) = tube.and_then(|t| self.tubes.get_mut(&t)) {
            t.stat.reserved_ct = t.stat.reserved_ct.saturating_sub(1);
        }
    }

    fn enqueue_waiting_conn(&mut self, cid: ConnId, wait_deadline: Option<Nanos>) {
        let watched = match self.conns.get_mut(&cid) {
            Some(c) => {
                c.waiting = true;
                c.wait_deadline = wait_deadline;
                c.watch.items.clone()
            }
            None => return,
        };
        self.waiting_ct += 1;
        for t in watched {
            if let Some(ts) = self.tubes.get_mut(&t) {
                ts.stat.waiting_ct += 1;
                ts.waiting_conns.append(cid);
            }
        }
    }

    fn do_remove_waiting_conn(&mut self, cid: ConnId) {
        let watched = match self.conns.get_mut(&cid) {
            Some(c) if c.waiting => {
                c.waiting = false;
                c.wait_deadline = None;
                c.watch.items.clone()
            }
            _ => return,
        };
        self.waiting_ct = self.waiting_ct.saturating_sub(1);
        for t in watched {
            if let Some(ts) = self.tubes.get_mut(&t) {
                ts.stat.waiting_ct = ts.stat.waiting_ct.saturating_sub(1);
                ts.waiting_conns.remove(&cid);
            }
        }
    }

    /// `process_queue`: repeatedly assigns the globally best (pri, id)
    /// ready job to a waiting connection, across every watched/unpaused
    /// tube, until no more assignments are possible. Mirrors
    /// `next_awaited_job`'s side effect of auto-clearing expired pauses.
    fn process_queue(&mut self, now: Nanos, out: &mut Outbox) {
        loop {
            let names: Vec<TubeName> = self.tube_order.items.clone();
            let mut best: Option<(u32, JobId, TubeName)> = None;
            for name in &names {
                let Some(t) = self.tubes.get_mut(name) else {
                    continue;
                };
                if t.pause > 0 {
                    if t.unpause_at > now {
                        continue;
                    }
                    t.pause = 0;
                }
                if !t.waiting_conns.is_empty()
                    && let Some(&(pri, id)) = t.ready.iter().next()
                {
                    let better = match &best {
                        None => true,
                        Some((bp, bid, _)) => (pri, id) < (*bp, *bid),
                    };
                    if better {
                        best = Some((pri, id, name.clone()));
                    }
                }
            }
            let Some((_, id, tube)) = best else { break };
            self.remove_ready(&tube, id);
            let cid = match self
                .tubes
                .get_mut(&tube)
                .and_then(|t| t.waiting_conns.take())
            {
                Some(c) => c,
                // Defensive: mirrors the reference's `if (c == NULL)` guard;
                // should not happen since we just checked non-empty above.
                None => continue,
            };
            self.do_remove_waiting_conn(cid);
            self.do_reserve(cid, id, now);
            let body = self
                .jobs
                .get(&id)
                .map(|j| j.body.clone())
                .unwrap_or_default();
            out.push((cid, Response::Reserved { id, body }));
        }
    }

    /// `soonest_delayed_job`: the delayed job with the smallest deadline
    /// across all tubes. Ties are broken by tube array order (first tube,
    /// in `tube_order` order, with the smallest deadline wins), matching
    /// the reference's strict `<` comparison over `tubes.items` in order.
    fn soonest_delayed_job(&self) -> Option<(Nanos, TubeName, JobId)> {
        let mut best: Option<(Nanos, TubeName, JobId)> = None;
        for name in &self.tube_order.items {
            let Some(t) = self.tubes.get(name) else {
                continue;
            };
            if let Some(&(deadline, id)) = t.delayed.iter().next() {
                let better = match &best {
                    None => true,
                    Some((bd, _, _)) => deadline < *bd,
                };
                if better {
                    best = Some((deadline, name.clone(), id));
                }
            }
        }
        best
    }

    fn conn_deadline_soon(&self, cid: ConnId, now: Nanos) -> bool {
        let Some(c) = self.conns.get(&cid) else {
            return false;
        };
        match c.soonest_reserved() {
            Some((deadline, _)) => now >= deadline.saturating_sub(SAFETY_MARGIN),
            None => false,
        }
    }

    fn conn_ready(&self, cid: ConnId) -> bool {
        let Some(c) = self.conns.get(&cid) else {
            return false;
        };
        c.watch.items.iter().any(|t| {
            self.tubes
                .get(t)
                .map(|ts| !ts.ready.is_empty())
                .unwrap_or(false)
        })
    }

    /// `conntickat`: the absolute time at which this connection next needs
    /// attention, if any.
    fn conn_tickat(&self, cid: ConnId) -> Option<Nanos> {
        let c = self.conns.get(&cid)?;
        let margin: i128 = if c.waiting { SAFETY_MARGIN as i128 } else { 0 };
        let mut t: Option<i128> = None;
        if let Some((deadline, _)) = c.soonest_reserved() {
            t = Some(deadline as i128 - margin);
        }
        if c.waiting
            && let Some(wd) = c.wait_deadline
        {
            t = Some(match t {
                Some(v) => v.min(wd as i128),
                None => wd as i128,
            });
        }
        t.map(|v| v.max(0) as Nanos)
    }

    /// `conn_timeout`: drains every reserved job of `cid` whose TTR has
    /// fully expired, then decides whether to emit DEADLINE_SOON or
    /// TIMED_OUT for a connection that is (still) waiting on reserve.
    ///
    /// Deliberate simplification vs. the reference: the DEADLINE_SOON /
    /// TIMED_OUT decision is (re)computed *after* the expiry loop, using
    /// live state, rather than snapshotting it before the loop runs. This
    /// is observably identical in every case except one pathological
    /// corner of the reference (a connection's own about-to-fully-expire
    /// job gets immediately re-reserved back to itself by `process_queue`
    /// inside the very same expiry pass, in which case the reference
    /// silently discards the RESERVED reply and sends DEADLINE_SOON
    /// instead). We consider that reference behavior a bug and do not
    /// reproduce it; see the T2 report for details.
    fn conn_timeout(&mut self, cid: ConnId, now: Nanos, out: &mut Outbox) {
        while let Some((deadline, job_id)) = self.conns.get(&cid).and_then(|c| c.soonest_reserved())
        {
            if deadline >= now {
                break;
            }
            let tube = self.jobs.get(&job_id).map(|j| j.tube.clone());
            self.do_unreserve(cid, job_id);
            self.timeout_ct += 1;
            if let Some(j) = self.jobs.get_mut(&job_id) {
                j.timeout_ct += 1;
            }
            if let Some(tube) = tube {
                self.insert_ready(&tube, job_id);
            }
            self.process_queue(now, out);
        }

        let waiting = self.conns.get(&cid).map(|c| c.waiting).unwrap_or(false);
        if waiting && self.conn_deadline_soon(cid, now) {
            self.do_remove_waiting_conn(cid);
            out.push((cid, Response::DeadlineSoon));
        } else if waiting {
            let expired = self
                .conns
                .get(&cid)
                .and_then(|c| c.wait_deadline)
                .map(|wd| wd <= now)
                .unwrap_or(false);
            if expired {
                self.do_remove_waiting_conn(cid);
                out.push((cid, Response::TimedOut));
            }
        }
    }

    fn connsetproducer(&mut self, cid: ConnId) {
        if let Some(c) = self.conns.get_mut(&cid)
            && !c.is_producer
        {
            c.is_producer = true;
            self.cur_producers += 1;
        }
    }

    fn connsetworker(&mut self, cid: ConnId) {
        if let Some(c) = self.conns.get_mut(&cid)
            && !c.is_worker
        {
            c.is_worker = true;
            self.cur_workers += 1;
        }
    }

    fn kick_to_ready(&mut self, tube: &TubeName, id: JobId, now: Nanos, out: &mut Outbox) {
        if let Some(j) = self.jobs.get_mut(&id) {
            j.kick_ct += 1;
        }
        self.insert_ready(tube, id);
        self.process_queue(now, out);
    }
}

// ---------------------------------------------------------------------
// Command handlers
// ---------------------------------------------------------------------
impl Engine {
    #[allow(clippy::too_many_arguments)]
    fn cmd_put(
        &mut self,
        now: Nanos,
        cid: ConnId,
        pri: u32,
        delay: u32,
        ttr: u32,
        body: Bytes,
        out: &mut Outbox,
    ) {
        self.cmd_put += 1;
        self.connsetproducer(cid);
        let ttr = ttr.max(1);

        // The reference always allocates a job id (bumping next_id) before
        // checking drain mode, even though a draining put discards the job
        // afterwards. We mirror that: the id is consumed either way.
        let id = self.next_job_id;
        self.next_job_id += 1;

        if self.draining {
            out.push((cid, Response::Draining));
            return;
        }

        let tube = self
            .conns
            .get(&cid)
            .map(|c| c.use_tube.clone())
            .unwrap_or_else(TubeName::default_tube);
        self.find_or_make_tube(&tube);

        let job = JobRec {
            id,
            tube: tube.clone(),
            pri,
            delay,
            ttr,
            body,
            created_at: now,
            deadline_at: 0,
            state: JobState::Ready,
            reserver: None,
            reserve_ct: 0,
            timeout_ct: 0,
            release_ct: 0,
            bury_ct: 0,
            kick_ct: 0,
        };
        self.jobs.insert(id, job);
        if let Some(t) = self.tubes.get_mut(&tube) {
            t.job_ref_ct += 1;
        }

        if delay > 0 {
            let deadline = now + (delay as Nanos) * NANOS_PER_SEC;
            self.insert_delayed(&tube, id, deadline);
        } else {
            self.insert_ready(&tube, id);
        }
        self.process_queue(now, out);

        self.total_jobs_ct += 1;
        if let Some(t) = self.tubes.get_mut(&tube) {
            t.stat.total_jobs_ct += 1;
        }
        out.push((cid, Response::Inserted(id)));
    }

    fn cmd_use(&mut self, cid: ConnId, tube: TubeName, out: &mut Outbox) {
        self.cmd_use += 1;
        self.find_or_make_tube(&tube);
        if let Some(old) = self.conns.get(&cid).map(|c| c.use_tube.clone())
            && old != tube
        {
            if let Some(t) = self.tubes.get_mut(&old) {
                t.using_ct = t.using_ct.saturating_sub(1);
            }
            self.gc_tube_if_orphan(&old);
            if let Some(t) = self.tubes.get_mut(&tube) {
                t.using_ct += 1;
            }
            if let Some(c) = self.conns.get_mut(&cid) {
                c.use_tube = tube.clone();
            }
        }
        out.push((cid, Response::Using(tube)));
    }

    fn cmd_watch(&mut self, cid: ConnId, tube: TubeName, out: &mut Outbox) {
        self.cmd_watch += 1;
        self.find_or_make_tube(&tube);
        let already = self
            .conns
            .get(&cid)
            .map(|c| c.watch.contains(&tube))
            .unwrap_or(true);
        if !already {
            if let Some(c) = self.conns.get_mut(&cid) {
                c.watch.append(tube.clone());
            }
            if let Some(t) = self.tubes.get_mut(&tube) {
                t.watching_ct += 1;
            }
        }
        let count = self.conns.get(&cid).map(|c| c.watch.len()).unwrap_or(0);
        out.push((cid, Response::Watching(count as u64)));
    }

    fn cmd_ignore(&mut self, cid: ConnId, tube: TubeName, out: &mut Outbox) {
        self.cmd_ignore += 1;
        let watching_it = self
            .conns
            .get(&cid)
            .map(|c| c.watch.contains(&tube))
            .unwrap_or(false);
        let watch_len = self.conns.get(&cid).map(|c| c.watch.len()).unwrap_or(0);
        if watching_it && watch_len < 2 {
            out.push((cid, Response::NotIgnored));
            return;
        }
        if watching_it {
            if let Some(c) = self.conns.get_mut(&cid) {
                c.watch.remove(&tube);
            }
            if let Some(t) = self.tubes.get_mut(&tube) {
                t.watching_ct = t.watching_ct.saturating_sub(1);
            }
            self.gc_tube_if_orphan(&tube);
        }
        let count = self.conns.get(&cid).map(|c| c.watch.len()).unwrap_or(0);
        out.push((cid, Response::Watching(count as u64)));
    }

    fn cmd_reserve(&mut self, now: Nanos, cid: ConnId, timeout: Option<u32>, out: &mut Outbox) {
        match timeout {
            None => self.cmd_reserve += 1,
            Some(_) => self.cmd_reserve_with_timeout += 1,
        }
        self.connsetworker(cid);
        if self.conn_deadline_soon(cid, now) && !self.conn_ready(cid) {
            out.push((cid, Response::DeadlineSoon));
            return;
        }
        let wait_deadline = timeout.map(|t| now + (t as Nanos) * NANOS_PER_SEC);
        let timeout_is_zero = timeout == Some(0);
        self.enqueue_waiting_conn(cid, wait_deadline);
        self.process_queue(now, out);
        let still_waiting = self.conns.get(&cid).map(|c| c.waiting).unwrap_or(false);
        if timeout_is_zero && still_waiting {
            self.do_remove_waiting_conn(cid);
            out.push((cid, Response::TimedOut));
        }
    }

    fn cmd_reserve_job(&mut self, now: Nanos, cid: ConnId, id: JobId, out: &mut Outbox) {
        let Some(state) = self.jobs.get(&id).map(|j| j.state) else {
            out.push((cid, Response::NotFound));
            return;
        };
        if state == JobState::Reserved {
            out.push((cid, Response::NotFound));
            return;
        }
        let tube = self
            .jobs
            .get(&id)
            .map(|j| j.tube.clone())
            .expect("job exists");
        if state == JobState::Ready {
            self.remove_ready(&tube, id);
        } else if state == JobState::Buried {
            self.remove_buried(&tube, id);
        } else {
            self.remove_delayed(&tube, id);
        }
        self.connsetworker(cid);
        self.do_reserve(cid, id, now);
        let body = self
            .jobs
            .get(&id)
            .map(|j| j.body.clone())
            .unwrap_or_default();
        out.push((cid, Response::Reserved { id, body }));
    }

    fn cmd_delete(&mut self, cid: ConnId, id: JobId, out: &mut Outbox) {
        self.cmd_delete += 1;
        let Some(state) = self.jobs.get(&id).map(|j| j.state) else {
            out.push((cid, Response::NotFound));
            return;
        };
        let reserver = self.jobs.get(&id).and_then(|j| j.reserver);
        let tube = self
            .jobs
            .get(&id)
            .map(|j| j.tube.clone())
            .expect("job exists");
        let ok = match state {
            JobState::Reserved => {
                if reserver == Some(cid) {
                    self.do_unreserve(cid, id);
                    true
                } else {
                    false
                }
            }
            JobState::Ready => {
                self.remove_ready(&tube, id);
                true
            }
            JobState::Buried => {
                self.remove_buried(&tube, id);
                true
            }
            JobState::Delayed => {
                self.remove_delayed(&tube, id);
                true
            }
        };
        if !ok {
            out.push((cid, Response::NotFound));
            return;
        }
        if let Some(t) = self.tubes.get_mut(&tube) {
            t.stat.total_delete_ct += 1;
            t.job_ref_ct = t.job_ref_ct.saturating_sub(1);
        }
        self.jobs.remove(&id);
        self.gc_tube_if_orphan(&tube);
        out.push((cid, Response::Deleted));
    }

    fn cmd_release(
        &mut self,
        now: Nanos,
        cid: ConnId,
        id: JobId,
        pri: u32,
        delay: u32,
        out: &mut Outbox,
    ) {
        self.cmd_release += 1;
        let reserved_by_me = self
            .jobs
            .get(&id)
            .map(|j| j.reserver == Some(cid) && j.state == JobState::Reserved)
            .unwrap_or(false);
        if !reserved_by_me {
            out.push((cid, Response::NotFound));
            return;
        }
        self.do_unreserve(cid, id);
        let tube = self
            .jobs
            .get(&id)
            .map(|j| j.tube.clone())
            .expect("job exists");
        if let Some(j) = self.jobs.get_mut(&id) {
            j.pri = pri;
            j.delay = delay;
            j.release_ct += 1;
        }
        if delay > 0 {
            let deadline = now + (delay as Nanos) * NANOS_PER_SEC;
            self.insert_delayed(&tube, id, deadline);
        } else {
            self.insert_ready(&tube, id);
        }
        self.process_queue(now, out);
        out.push((cid, Response::Released));
    }

    fn cmd_bury(&mut self, cid: ConnId, id: JobId, pri: u32, out: &mut Outbox) {
        self.cmd_bury += 1;
        let reserved_by_me = self
            .jobs
            .get(&id)
            .map(|j| j.reserver == Some(cid) && j.state == JobState::Reserved)
            .unwrap_or(false);
        if !reserved_by_me {
            out.push((cid, Response::NotFound));
            return;
        }
        self.do_unreserve(cid, id);
        let tube = self
            .jobs
            .get(&id)
            .map(|j| j.tube.clone())
            .expect("job exists");
        if let Some(j) = self.jobs.get_mut(&id) {
            j.pri = pri;
        }
        self.insert_buried(&tube, id);
        out.push((cid, Response::Buried));
    }

    fn cmd_touch(&mut self, now: Nanos, cid: ConnId, id: JobId, out: &mut Outbox) {
        self.cmd_touch += 1;
        let reserved_by_me = self
            .jobs
            .get(&id)
            .map(|j| j.reserver == Some(cid) && j.state == JobState::Reserved)
            .unwrap_or(false);
        if !reserved_by_me {
            out.push((cid, Response::NotFound));
            return;
        }
        let ttr = self.jobs.get(&id).map(|j| j.ttr).unwrap_or(1);
        let old_deadline = self.jobs.get(&id).map(|j| j.deadline_at).unwrap_or(0);
        let new_deadline = now + (ttr as Nanos) * NANOS_PER_SEC;
        if let Some(j) = self.jobs.get_mut(&id) {
            j.deadline_at = new_deadline;
        }
        if let Some(c) = self.conns.get_mut(&cid) {
            c.reserved_by_deadline.remove(&(old_deadline, id));
            c.reserved_by_deadline.insert((new_deadline, id));
        }
        out.push((cid, Response::Touched));
    }

    fn cmd_peek(&mut self, cid: ConnId, id: JobId, out: &mut Outbox) {
        self.cmd_peek += 1;
        match self.jobs.get(&id) {
            Some(j) => out.push((
                cid,
                Response::Found {
                    id,
                    body: j.body.clone(),
                },
            )),
            None => out.push((cid, Response::NotFound)),
        }
    }

    fn cmd_peek_ready(&mut self, cid: ConnId, out: &mut Outbox) {
        self.cmd_peek_ready += 1;
        let tube = self
            .conns
            .get(&cid)
            .map(|c| c.use_tube.clone())
            .unwrap_or_else(TubeName::default_tube);
        let top = self
            .tubes
            .get(&tube)
            .and_then(|t| t.ready.iter().next().copied());
        match top {
            Some((_, id)) => {
                let body = self
                    .jobs
                    .get(&id)
                    .map(|j| j.body.clone())
                    .unwrap_or_default();
                out.push((cid, Response::Found { id, body }));
            }
            None => out.push((cid, Response::NotFound)),
        }
    }

    fn cmd_peek_delayed(&mut self, cid: ConnId, out: &mut Outbox) {
        self.cmd_peek_delayed += 1;
        let tube = self
            .conns
            .get(&cid)
            .map(|c| c.use_tube.clone())
            .unwrap_or_else(TubeName::default_tube);
        let top = self
            .tubes
            .get(&tube)
            .and_then(|t| t.delayed.iter().next().copied());
        match top {
            Some((_, id)) => {
                let body = self
                    .jobs
                    .get(&id)
                    .map(|j| j.body.clone())
                    .unwrap_or_default();
                out.push((cid, Response::Found { id, body }));
            }
            None => out.push((cid, Response::NotFound)),
        }
    }

    fn cmd_peek_buried(&mut self, cid: ConnId, out: &mut Outbox) {
        self.cmd_peek_buried += 1;
        let tube = self
            .conns
            .get(&cid)
            .map(|c| c.use_tube.clone())
            .unwrap_or_else(TubeName::default_tube);
        let top = self
            .tubes
            .get(&tube)
            .and_then(|t| t.buried.front().copied());
        match top {
            Some(id) => {
                let body = self
                    .jobs
                    .get(&id)
                    .map(|j| j.body.clone())
                    .unwrap_or_default();
                out.push((cid, Response::Found { id, body }));
            }
            None => out.push((cid, Response::NotFound)),
        }
    }

    fn cmd_kick(&mut self, now: Nanos, cid: ConnId, n: u32, out: &mut Outbox) {
        self.cmd_kick += 1;
        let tube = self
            .conns
            .get(&cid)
            .map(|c| c.use_tube.clone())
            .unwrap_or_else(TubeName::default_tube);
        let has_buried = self
            .tubes
            .get(&tube)
            .map(|t| !t.buried.is_empty())
            .unwrap_or(false);
        let mut count: u64 = 0;
        if has_buried {
            for _ in 0..n {
                let Some(id) = self.pop_buried_front(&tube) else {
                    break;
                };
                self.kick_to_ready(&tube, id, now, out);
                count += 1;
            }
        } else {
            for _ in 0..n {
                let next = self
                    .tubes
                    .get(&tube)
                    .and_then(|t| t.delayed.iter().next().copied());
                let Some((_, id)) = next else { break };
                self.remove_delayed(&tube, id);
                self.kick_to_ready(&tube, id, now, out);
                count += 1;
            }
        }
        out.push((cid, Response::Kicked(count)));
    }

    fn cmd_kick_job(&mut self, now: Nanos, cid: ConnId, id: JobId, out: &mut Outbox) {
        let Some(state) = self.jobs.get(&id).map(|j| j.state) else {
            out.push((cid, Response::NotFound));
            return;
        };
        let tube = self
            .jobs
            .get(&id)
            .map(|j| j.tube.clone())
            .expect("job exists");
        match state {
            JobState::Buried => {
                self.remove_buried(&tube, id);
                self.kick_to_ready(&tube, id, now, out);
                out.push((cid, Response::KickedJob));
            }
            JobState::Delayed => {
                self.remove_delayed(&tube, id);
                self.kick_to_ready(&tube, id, now, out);
                out.push((cid, Response::KickedJob));
            }
            _ => out.push((cid, Response::NotFound)),
        }
    }

    fn cmd_stats_job(&mut self, now: Nanos, cid: ConnId, id: JobId, out: &mut Outbox) {
        self.cmd_stats_job += 1;
        match self.build_stats_job(id, now) {
            Some(s) => out.push((cid, Response::Ok(s.to_yaml()))),
            None => out.push((cid, Response::NotFound)),
        }
    }

    fn cmd_stats_tube(&mut self, now: Nanos, cid: ConnId, tube: TubeName, out: &mut Outbox) {
        self.cmd_stats_tube += 1;
        match self.build_stats_tube(&tube, now) {
            Some(s) => out.push((cid, Response::Ok(s.to_yaml()))),
            None => out.push((cid, Response::NotFound)),
        }
    }

    fn cmd_stats(&mut self, now: Nanos, cid: ConnId, out: &mut Outbox) {
        self.cmd_stats += 1;
        let s = self.build_stats_server(now);
        out.push((cid, Response::Ok(s.to_yaml())));
    }

    fn cmd_list_tubes(&mut self, cid: ConnId, out: &mut Outbox) {
        self.cmd_list_tubes += 1;
        let names = self.tube_names();
        out.push((cid, Response::Ok(bstk_proto::yaml_list(names.iter()))));
    }

    fn cmd_list_tube_used(&mut self, cid: ConnId, out: &mut Outbox) {
        self.cmd_list_tube_used += 1;
        let tube = self
            .conns
            .get(&cid)
            .map(|c| c.use_tube.clone())
            .unwrap_or_else(TubeName::default_tube);
        out.push((cid, Response::Using(tube)));
    }

    fn cmd_list_tubes_watched(&mut self, cid: ConnId, out: &mut Outbox) {
        self.cmd_list_tubes_watched += 1;
        let names = self.watched_tube_names(cid);
        out.push((cid, Response::Ok(bstk_proto::yaml_list(names.iter()))));
    }

    fn cmd_pause_tube(
        &mut self,
        now: Nanos,
        cid: ConnId,
        tube: TubeName,
        delay: u32,
        out: &mut Outbox,
    ) {
        self.cmd_pause_tube += 1;
        if !self.tubes.contains_key(&tube) {
            out.push((cid, Response::NotFound));
            return;
        }
        let delay = delay.max(1);
        let delay_nanos = (delay as Nanos) * NANOS_PER_SEC;
        if let Some(t) = self.tubes.get_mut(&tube) {
            t.pause = delay_nanos;
            t.unpause_at = now + delay_nanos;
            t.stat.pause_ct += 1;
        }
        out.push((cid, Response::Paused));
    }
}

// ---------------------------------------------------------------------
// Stats / introspection builders. `pub(crate)` only: this does not widen
// the public API. Kept separate from `to_yaml()` so tests can assert on
// struct fields directly without depending on T1's YAML formatting.
// ---------------------------------------------------------------------
impl Engine {
    pub(crate) fn tube_names(&self) -> Vec<TubeName> {
        self.tube_order.items.clone()
    }

    pub(crate) fn watched_tube_names(&self, cid: ConnId) -> Vec<TubeName> {
        self.conns
            .get(&cid)
            .map(|c| c.watch.items.clone())
            .unwrap_or_default()
    }

    pub(crate) fn build_stats_job(&self, id: JobId, now: Nanos) -> Option<StatsJob> {
        let j = self.jobs.get(&id)?;
        let time_left = match j.state {
            JobState::Reserved | JobState::Delayed => {
                let diff = j.deadline_at as i128 - now as i128;
                (diff / NANOS_PER_SEC as i128).max(0) as u64
            }
            _ => 0,
        };
        let age = ((now as i128 - j.created_at as i128) / NANOS_PER_SEC as i128).max(0) as u64;
        Some(StatsJob {
            id: j.id,
            tube: j.tube.clone(),
            state: j.state_name(),
            pri: j.pri,
            age,
            delay: j.delay as u64,
            ttr: j.ttr as u64,
            time_left,
            file: 0,
            reserves: j.reserve_ct as u64,
            timeouts: j.timeout_ct as u64,
            releases: j.release_ct as u64,
            buries: j.bury_ct as u64,
            kicks: j.kick_ct as u64,
        })
    }

    pub(crate) fn build_stats_tube(&self, name: &TubeName, now: Nanos) -> Option<StatsTube> {
        let t = self.tubes.get(name)?;
        let pause_time_left = if t.pause > 0 {
            t.unpause_at.saturating_sub(now) / NANOS_PER_SEC
        } else {
            0
        };
        Some(StatsTube {
            name: t.name.clone(),
            current_jobs_urgent: t.stat.urgent_ct,
            current_jobs_ready: t.ready.len() as u64,
            current_jobs_reserved: t.stat.reserved_ct,
            current_jobs_delayed: t.delayed.len() as u64,
            current_jobs_buried: t.stat.buried_ct,
            total_jobs: t.stat.total_jobs_ct,
            current_using: t.using_ct as u64,
            current_watching: t.watching_ct as u64,
            current_waiting: t.stat.waiting_ct,
            cmd_delete: t.stat.total_delete_ct,
            cmd_pause_tube: t.stat.pause_ct,
            pause: t.pause / NANOS_PER_SEC,
            pause_time_left,
        })
    }

    pub(crate) fn build_stats_server(&self, now: Nanos) -> StatsServer {
        let snap = self.sys.snapshot();
        let current_jobs_delayed: u64 = self.tubes.values().map(|t| t.delayed.len() as u64).sum();
        StatsServer {
            current_jobs_urgent: self.urgent_ct,
            current_jobs_ready: self.ready_ct,
            current_jobs_reserved: self.reserved_ct,
            current_jobs_delayed,
            current_jobs_buried: self.buried_ct,
            cmd_put: self.cmd_put,
            cmd_peek: self.cmd_peek,
            cmd_peek_ready: self.cmd_peek_ready,
            cmd_peek_delayed: self.cmd_peek_delayed,
            cmd_peek_buried: self.cmd_peek_buried,
            cmd_reserve: self.cmd_reserve,
            cmd_reserve_with_timeout: self.cmd_reserve_with_timeout,
            cmd_delete: self.cmd_delete,
            cmd_release: self.cmd_release,
            cmd_use: self.cmd_use,
            cmd_watch: self.cmd_watch,
            cmd_ignore: self.cmd_ignore,
            cmd_bury: self.cmd_bury,
            cmd_kick: self.cmd_kick,
            cmd_touch: self.cmd_touch,
            cmd_stats: self.cmd_stats,
            cmd_stats_job: self.cmd_stats_job,
            cmd_stats_tube: self.cmd_stats_tube,
            cmd_list_tubes: self.cmd_list_tubes,
            cmd_list_tube_used: self.cmd_list_tube_used,
            cmd_list_tubes_watched: self.cmd_list_tubes_watched,
            cmd_pause_tube: self.cmd_pause_tube,
            job_timeouts: self.timeout_ct,
            total_jobs: self.total_jobs_ct,
            max_job_size: self.cfg.max_job_size as u64,
            current_tubes: self.tube_order.len() as u64,
            current_connections: self.cur_conns as u64,
            current_producers: self.cur_producers as u64,
            current_workers: self.cur_workers as u64,
            current_waiting: self.waiting_ct,
            total_connections: self.tot_conns as u64,
            pid: snap.pid,
            version: snap.version,
            rusage_utime: snap.rusage_utime,
            rusage_stime: snap.rusage_stime,
            uptime: now.saturating_sub(self.start) / NANOS_PER_SEC,
            binlog_oldest_index: 0,
            binlog_current_index: 0,
            binlog_records_migrated: 0,
            binlog_records_written: 0,
            binlog_max_size: self.cfg.binlog_max_size,
            draining: self.draining,
            id: snap.id,
            hostname: snap.hostname,
            os: snap.os,
            platform: snap.platform,
        }
    }
}

// ---------------------------------------------------------------------
// Test-only introspection. Never widens the public API (all pub(crate)).
// ---------------------------------------------------------------------
#[cfg(test)]
impl Engine {
    pub(crate) fn t_job_state(&self, id: JobId) -> Option<&'static str> {
        self.jobs.get(&id).map(|j| j.state_name())
    }

    pub(crate) fn t_job_reserver(&self, id: JobId) -> Option<ConnId> {
        self.jobs.get(&id).and_then(|j| j.reserver)
    }

    pub(crate) fn t_job_exists(&self, id: JobId) -> bool {
        self.jobs.contains_key(&id)
    }

    pub(crate) fn t_job_pri(&self, id: JobId) -> Option<u32> {
        self.jobs.get(&id).map(|j| j.pri)
    }

    pub(crate) fn t_ready_ct(&self) -> u64 {
        self.ready_ct
    }

    pub(crate) fn t_reserved_ct(&self) -> u64 {
        self.reserved_ct
    }

    pub(crate) fn t_buried_ct(&self) -> u64 {
        self.buried_ct
    }

    pub(crate) fn t_waiting_ct(&self) -> u64 {
        self.waiting_ct
    }

    pub(crate) fn t_tube_ready_len(&self, name: &TubeName) -> Option<usize> {
        self.tubes.get(name).map(|t| t.ready.len())
    }

    pub(crate) fn t_tube_ready_ids(&self, name: &TubeName) -> Vec<JobId> {
        self.tubes
            .get(name)
            .map(|t| t.ready.iter().map(|&(_, id)| id).collect())
            .unwrap_or_default()
    }

    pub(crate) fn t_tube_delayed_ids(&self, name: &TubeName) -> Vec<JobId> {
        self.tubes
            .get(name)
            .map(|t| t.delayed.iter().map(|&(_, id)| id).collect())
            .unwrap_or_default()
    }

    pub(crate) fn t_tube_buried_ids(&self, name: &TubeName) -> Vec<JobId> {
        self.tubes
            .get(name)
            .map(|t| t.buried.iter().copied().collect())
            .unwrap_or_default()
    }

    pub(crate) fn t_tube_paused(&self, name: &TubeName) -> Option<bool> {
        self.tubes.get(name).map(|t| t.pause > 0)
    }

    pub(crate) fn t_tube_delayed_len(&self, name: &TubeName) -> Option<usize> {
        self.tubes.get(name).map(|t| t.delayed.len())
    }

    pub(crate) fn t_tube_buried_len(&self, name: &TubeName) -> Option<usize> {
        self.tubes.get(name).map(|t| t.buried.len())
    }

    pub(crate) fn t_tube_waiting_conns(&self, name: &TubeName) -> Option<usize> {
        self.tubes.get(name).map(|t| t.waiting_conns.len())
    }

    pub(crate) fn t_tube_exists(&self, name: &TubeName) -> bool {
        self.tubes.contains_key(name)
    }

    pub(crate) fn t_conn_exists(&self, cid: ConnId) -> bool {
        self.conns.contains_key(&cid)
    }

    pub(crate) fn t_conn_waiting(&self, cid: ConnId) -> bool {
        self.conns.get(&cid).map(|c| c.waiting).unwrap_or(false)
    }

    pub(crate) fn t_conn_reserved(&self, cid: ConnId) -> Vec<JobId> {
        self.conns
            .get(&cid)
            .map(|c| c.reserved_fifo.clone())
            .unwrap_or_default()
    }

    pub(crate) fn t_all_job_ids(&self) -> Vec<JobId> {
        self.jobs.keys().copied().collect()
    }

    pub(crate) fn t_all_tube_names(&self) -> Vec<TubeName> {
        self.tubes.keys().cloned().collect()
    }

    pub(crate) fn t_all_conn_ids(&self) -> Vec<ConnId> {
        self.conns.keys().copied().collect()
    }

    pub(crate) fn t_cur_conns(&self) -> u32 {
        self.cur_conns
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use bytes::Bytes;

    use bstk_proto::{Command, Response, TubeName};

    use crate::{EngineConfig, Nanos, Outbox, StaticSysInfo};

    use super::Engine;

    const SEC: Nanos = crate::NANOS_PER_SEC;

    fn engine_at(now: Nanos) -> Engine {
        Engine::new(
            now,
            EngineConfig::default(),
            Box::new(StaticSysInfo::default()),
        )
    }

    fn tube(name: &str) -> TubeName {
        TubeName::new(name).unwrap()
    }

    // Verified against the reference: a rejected put still bumps cmd-put;
    // EXPECTED_CRLF also consumes a job id and marks the conn a producer.
    #[test]
    fn put_rejected_side_effects_match_reference() {
        use bstk_proto::PutRejection;
        let mut e = engine_at(0);
        e.connect(0, 1);
        let mut out = Outbox::new();
        e.put_rejected(0, 1, PutRejection::JobTooBig, &mut out);
        e.put_rejected(0, 1, PutRejection::TrailingGarbage, &mut out);
        assert_eq!(
            out,
            vec![(1, Response::JobTooBig), (1, Response::BadFormat)]
        );
        let s = e.build_stats_server(0);
        assert_eq!((s.cmd_put, s.current_producers, s.total_jobs), (2, 0, 0));

        out.clear();
        e.put_rejected(0, 1, PutRejection::ExpectedCrlf, &mut out);
        assert_eq!(out, vec![(1, Response::ExpectedCrlf)]);
        let s = e.build_stats_server(0);
        assert_eq!((s.cmd_put, s.current_producers, s.total_jobs), (3, 1, 0));
        assert_eq!(put(&mut e, 0, 1, 0, 0, 10, "x"), 2);
    }

    fn put(
        e: &mut Engine,
        now: Nanos,
        cid: u64,
        pri: u32,
        delay: u32,
        ttr: u32,
        body: &str,
    ) -> u64 {
        let mut out = Outbox::new();
        e.handle(
            now,
            cid,
            Command::Put {
                pri,
                delay,
                ttr,
                body: Bytes::copy_from_slice(body.as_bytes()),
            },
            &mut out,
        );
        match only(&out, cid) {
            Response::Inserted(id) => id,
            other => panic!("expected Inserted, got {other:?}"),
        }
    }

    /// Asserts exactly one reply was produced for `cid` and returns it.
    fn only(out: &Outbox, cid: u64) -> Response {
        let matches: Vec<&Response> = out
            .iter()
            .filter(|(c, _)| *c == cid)
            .map(|(_, r)| r)
            .collect();
        assert_eq!(
            matches.len(),
            1,
            "expected exactly one reply for conn {cid} in {out:?}"
        );
        matches[0].clone()
    }

    /// Asserts no reply was produced for `cid`.
    fn none_for(out: &Outbox, cid: u64) {
        assert!(
            out.iter().all(|(c, _)| *c != cid),
            "expected no reply for conn {cid}, got {out:?}"
        );
    }

    fn handle(e: &mut Engine, now: Nanos, cid: u64, cmd: Command) -> Outbox {
        let mut out = Outbox::new();
        e.handle(now, cid, cmd, &mut out);
        out
    }

    // -----------------------------------------------------------------
    // connect / put / basic tube plumbing
    // -----------------------------------------------------------------

    #[test]
    fn connect_uses_and_watches_default() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        assert_eq!(e.watched_tube_names(1), vec![tube("default")]);
        assert!(e.t_conn_exists(1));
        assert_eq!(e.t_cur_conns(), 1);
    }

    #[test]
    fn put_assigns_sequential_ids_and_defaults_ttr() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        let id1 = put(&mut e, 0, 1, 0, 0, 0, "a");
        let id2 = put(&mut e, 0, 1, 0, 0, 0, "b");
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        // ttr=0 is bumped to 1 second.
        let stats = e.build_stats_job(id1, 0).unwrap();
        assert_eq!(stats.ttr, 1);
        assert_eq!(e.t_job_state(id1), Some("ready"));
    }

    #[test]
    fn put_with_delay_goes_to_delayed_state() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        let id = put(&mut e, 0, 1, 0, 5, 0, "a");
        assert_eq!(e.t_job_state(id), Some("delayed"));
        assert_eq!(e.t_tube_delayed_len(&tube("default")), Some(1));
        assert_eq!(e.t_tube_ready_len(&tube("default")), Some(0));
    }

    #[test]
    fn draining_rejects_put_but_still_consumes_the_id() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        e.set_draining(true);
        let out = handle(
            &mut e,
            0,
            1,
            Command::Put {
                pri: 0,
                delay: 0,
                ttr: 0,
                body: Bytes::from_static(b"x"),
            },
        );
        assert_eq!(only(&out, 1), Response::Draining);
        assert!(!e.t_job_exists(1));
        e.set_draining(false);
        // The id that would have gone to the drained put is skipped.
        let id = put(&mut e, 0, 1, 0, 0, 0, "y");
        assert_eq!(id, 2);
    }

    #[test]
    fn use_switches_tube_and_replies_using() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        let out = handle(&mut e, 0, 1, Command::Use(tube("foo")));
        assert_eq!(only(&out, 1), Response::Using(tube("foo")));
        let out = handle(&mut e, 0, 1, Command::ListTubeUsed);
        assert_eq!(only(&out, 1), Response::Using(tube("foo")));
    }

    #[test]
    fn watch_and_ignore_update_watching_count_and_reply() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        let out = handle(&mut e, 0, 1, Command::Watch(tube("foo")));
        assert_eq!(only(&out, 1), Response::Watching(2));
        // Re-watching the same tube does not double-count.
        let out = handle(&mut e, 0, 1, Command::Watch(tube("foo")));
        assert_eq!(only(&out, 1), Response::Watching(2));

        let out = handle(&mut e, 0, 1, Command::Ignore(tube("default")));
        assert_eq!(only(&out, 1), Response::Watching(1));
    }

    #[test]
    fn ignore_last_watched_tube_is_refused() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        let out = handle(&mut e, 0, 1, Command::Ignore(tube("default")));
        assert_eq!(only(&out, 1), Response::NotIgnored);
    }

    #[test]
    fn ignore_unwatched_tube_is_a_silent_noop() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        // "foo" is not being watched at all; reference still replies with
        // the current (unchanged) watch count instead of an error.
        let out = handle(&mut e, 0, 1, Command::Ignore(tube("foo")));
        assert_eq!(only(&out, 1), Response::Watching(1));
    }

    #[test]
    fn list_tubes_watched_order_survives_swap_remove_on_ignore() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        for name in ["a", "b", "c"] {
            handle(&mut e, 0, 1, Command::Watch(tube(name)));
        }
        // watch order: default, a, b, c
        assert_eq!(
            e.watched_tube_names(1),
            vec![tube("default"), tube("a"), tube("b"), tube("c")]
        );
        // ms_remove swaps the removed item with the last one.
        handle(&mut e, 0, 1, Command::Ignore(tube("a")));
        assert_eq!(
            e.watched_tube_names(1),
            vec![tube("default"), tube("c"), tube("b")]
        );
    }

    #[test]
    fn tube_is_garbage_collected_when_unreferenced() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        handle(&mut e, 0, 1, Command::Watch(tube("foo")));
        assert!(e.t_tube_exists(&tube("foo")));
        handle(&mut e, 0, 1, Command::Ignore(tube("foo")));
        assert!(!e.t_tube_exists(&tube("foo")));
    }

    #[test]
    fn default_tube_is_never_garbage_collected() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        e.disconnect(0, 1, &mut Outbox::new());
        // No connections left at all, yet "default" must still exist.
        assert!(e.t_tube_exists(&tube("default")));
    }

    #[test]
    fn tube_stays_alive_while_it_still_has_a_job() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        handle(&mut e, 0, 1, Command::Use(tube("foo")));
        let id = put(&mut e, 0, 1, 0, 0, 0, "x");
        // Switch away and disconnect; the tube must survive because of the job.
        handle(&mut e, 0, 1, Command::Use(tube("default")));
        e.disconnect(0, 1, &mut Outbox::new());
        assert!(e.t_tube_exists(&tube("foo")));
        // Once the job is gone (via a fresh connection deleting it), it's collected.
        e.connect(0, 2);
        handle(&mut e, 0, 2, Command::Delete(id));
        assert!(!e.t_tube_exists(&tube("foo")));
    }

    // -----------------------------------------------------------------
    // reserve / reserve-with-timeout / reserve-job
    // -----------------------------------------------------------------

    #[test]
    fn reserve_returns_ready_job_immediately() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        let id = put(&mut e, 0, 1, 0, 0, 0, "hi");
        let out = handle(&mut e, 0, 1, Command::Reserve);
        assert_eq!(
            only(&out, 1),
            Response::Reserved {
                id,
                body: Bytes::from_static(b"hi")
            }
        );
        assert_eq!(e.t_job_state(id), Some("reserved"));
        assert_eq!(e.t_job_reserver(id), Some(1));
    }

    #[test]
    fn reserve_blocks_then_is_woken_by_a_later_put() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        e.connect(0, 2);
        let out = handle(&mut e, 0, 1, Command::Reserve);
        none_for(&out, 1);
        assert!(e.t_conn_waiting(1));

        let out = handle(
            &mut e,
            0,
            2,
            Command::Put {
                pri: 0,
                delay: 0,
                ttr: 0,
                body: Bytes::from_static(b"woke"),
            },
        );
        let id = match only(&out, 2) {
            Response::Inserted(id) => id,
            other => panic!("expected Inserted, got {other:?}"),
        };
        assert_eq!(
            only(&out, 1),
            Response::Reserved {
                id,
                body: Bytes::from_static(b"woke")
            }
        );
        assert!(!e.t_conn_waiting(1));
    }

    #[test]
    fn reserve_with_timeout_zero_returns_timed_out_when_nothing_ready() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        let out = handle(&mut e, 0, 1, Command::ReserveWithTimeout(0));
        assert_eq!(only(&out, 1), Response::TimedOut);
        assert!(!e.t_conn_waiting(1));
        assert_eq!(e.t_waiting_ct(), 0);
    }

    #[test]
    fn reserve_with_timeout_zero_returns_job_when_ready() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        let id = put(&mut e, 0, 1, 0, 0, 0, "x");
        let out = handle(&mut e, 0, 1, Command::ReserveWithTimeout(0));
        assert_eq!(
            only(&out, 1),
            Response::Reserved {
                id,
                body: Bytes::from_static(b"x")
            }
        );
    }

    #[test]
    fn reserve_with_timeout_positive_times_out_via_tick() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        let out = handle(&mut e, 0, 1, Command::ReserveWithTimeout(5));
        none_for(&out, 1);
        assert_eq!(e.next_deadline(), Some(5 * SEC));

        let mut out = Outbox::new();
        e.tick(5 * SEC, &mut out);
        assert_eq!(only(&out, 1), Response::TimedOut);
        assert!(!e.t_conn_waiting(1));
    }

    #[test]
    fn reserve_job_works_on_ready_delayed_and_buried() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        e.connect(0, 2);

        let ready_id = put(&mut e, 0, 1, 0, 0, 0, "r");
        let delayed_id = put(&mut e, 0, 1, 0, 100, 0, "d");
        let buried_id = put(&mut e, 0, 1, 0, 0, 0, "b");
        // conn 1 reserves buried_id then buries it.
        handle(&mut e, 0, 1, Command::ReserveJob(buried_id));
        handle(
            &mut e,
            0,
            1,
            Command::Bury {
                id: buried_id,
                pri: 0,
            },
        );

        for id in [ready_id, delayed_id, buried_id] {
            let out = handle(&mut e, 0, 2, Command::ReserveJob(id));
            match only(&out, 2) {
                Response::Reserved { id: rid, .. } => assert_eq!(rid, id),
                other => panic!("expected Reserved for {id}, got {other:?}"),
            }
        }
    }

    #[test]
    fn reserve_job_not_found_for_missing_or_already_reserved() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        e.connect(0, 2);
        let out = handle(&mut e, 0, 1, Command::ReserveJob(999));
        assert_eq!(only(&out, 1), Response::NotFound);

        let id = put(&mut e, 0, 1, 0, 0, 0, "x");
        handle(&mut e, 0, 1, Command::ReserveJob(id));
        let out = handle(&mut e, 0, 2, Command::ReserveJob(id));
        assert_eq!(only(&out, 2), Response::NotFound);
    }

    // -----------------------------------------------------------------
    // deadline-soon (both triggers)
    // -----------------------------------------------------------------

    #[test]
    fn deadline_soon_on_reserve_when_no_job_ready() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        // ttr=2s; margin is 1s, so at t=1s we're within the margin.
        let id = put(&mut e, 0, 1, 0, 0, 2, "x");
        handle(&mut e, 0, 1, Command::ReserveJob(id));
        let out = handle(&mut e, SEC, 1, Command::Reserve);
        assert_eq!(only(&out, 1), Response::DeadlineSoon);
    }

    #[test]
    fn deadline_soon_shortcut_is_skipped_when_a_job_is_ready() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        let held = put(&mut e, 0, 1, 0, 0, 2, "held");
        handle(&mut e, 0, 1, Command::ReserveJob(held));
        // Another job is immediately available, so reserve succeeds instead
        // of short-circuiting to DEADLINE_SOON.
        let other = put(&mut e, 0, 1, 0, 0, 0, "other");
        let out = handle(&mut e, SEC, 1, Command::Reserve);
        assert_eq!(
            only(&out, 1),
            Response::Reserved {
                id: other,
                body: Bytes::from_static(b"other")
            }
        );
    }

    /// Empirically verified against the reference binary: when the
    /// shortcut is skipped because a ready job exists on a *paused* tube
    /// (so `conn_ready` is true but `process_queue` can't actually hand it
    /// out), the connection is registered as waiting -- and since it was
    /// already past the margin at that very moment, an immediate `tick`
    /// call at the *same* `now` resolves it to DEADLINE_SOON right away,
    /// exactly like the reference's near-zero-latency event loop
    /// re-entering `prottick` right after `dispatch_cmd`. This means
    /// bstk-server (T4) MUST call `tick(now)` immediately after every
    /// `handle()`, not only when `next_deadline()` says so, or this
    /// class of DEADLINE_SOON would be delayed compared to the reference.
    #[test]
    fn deadline_soon_shortcut_skip_on_paused_ready_job_resolves_on_immediate_tick() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        handle(&mut e, 0, 1, Command::Use(tube("margin_test")));
        handle(&mut e, 0, 1, Command::Watch(tube("margin_test")));
        let held = put(&mut e, 0, 1, 0, 0, 2, "held");
        handle(&mut e, 0, 1, Command::ReserveJob(held));

        handle(
            &mut e,
            0,
            1,
            Command::PauseTube {
                tube: tube("margin_test"),
                delay: 100,
            },
        );
        put(&mut e, 0, 1, 0, 0, 60, "other");

        // Within the margin (deadline=2s, margin=1s): shortcut is skipped
        // because conn_ready() sees the paused tube's ready job, ignoring
        // pause -- matching the reference's conn_ready(), which also does
        // not check pause.
        let out = handle(&mut e, SEC, 1, Command::Reserve);
        none_for(&out, 1);
        assert!(e.t_conn_waiting(1));

        // But it was already overdue for the margin the instant it started
        // waiting, so next_deadline() says "now" (or earlier), and an
        // immediate tick at the same `now` resolves it to DEADLINE_SOON.
        assert_eq!(e.next_deadline(), Some(SEC));
        let mut out = Outbox::new();
        e.tick(SEC, &mut out);
        assert_eq!(only(&out, 1), Response::DeadlineSoon);
        assert!(!e.t_conn_waiting(1));
    }

    #[test]
    fn deadline_soon_fires_for_a_waiting_connection_entering_the_margin() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        e.connect(0, 2);
        // conn 1 holds job A (ttr=3s) and is also waiting for a new job on a
        // tube with nothing ready.
        let held = put(&mut e, 0, 1, 0, 0, 3, "held");
        handle(&mut e, 0, 1, Command::ReserveJob(held));
        let out = handle(&mut e, 0, 1, Command::Reserve);
        none_for(&out, 1);

        // Margin point: deadline(3s) - 1s = 2s.
        assert_eq!(e.next_deadline(), Some(2 * SEC));
        let mut out = Outbox::new();
        e.tick(2 * SEC, &mut out);
        assert_eq!(only(&out, 1), Response::DeadlineSoon);
        // The held job is untouched: still reserved, not requeued/expired.
        assert_eq!(e.t_job_state(held), Some("reserved"));
        assert!(!e.t_conn_waiting(1));

        // Later, full TTR expiry happens on its own with no further reply
        // to conn 1 (it's no longer waiting). Note: like the reference
        // (`if (j->r.deadline_at >= nanoseconds()) break;`), expiry requires
        // strictly *passing* the deadline, so we tick 1ns past it.
        let mut out = Outbox::new();
        e.tick(3 * SEC + 1, &mut out);
        none_for(&out, 1);
        assert_eq!(e.t_job_state(held), Some("ready"));
    }

    // -----------------------------------------------------------------
    // delete / release / bury / touch
    // -----------------------------------------------------------------

    #[test]
    fn delete_removes_from_every_state() {
        let mut e = engine_at(0);
        e.connect(0, 1);

        let ready_id = put(&mut e, 0, 1, 0, 0, 0, "r");
        let out = handle(&mut e, 0, 1, Command::Delete(ready_id));
        assert_eq!(only(&out, 1), Response::Deleted);
        assert!(!e.t_job_exists(ready_id));

        let delayed_id = put(&mut e, 0, 1, 0, 100, 0, "d");
        let out = handle(&mut e, 0, 1, Command::Delete(delayed_id));
        assert_eq!(only(&out, 1), Response::Deleted);

        let buried_id = put(&mut e, 0, 1, 0, 0, 0, "b");
        handle(&mut e, 0, 1, Command::ReserveJob(buried_id));
        handle(
            &mut e,
            0,
            1,
            Command::Bury {
                id: buried_id,
                pri: 0,
            },
        );
        let out = handle(&mut e, 0, 1, Command::Delete(buried_id));
        assert_eq!(only(&out, 1), Response::Deleted);

        let reserved_id = put(&mut e, 0, 1, 0, 0, 0, "v");
        handle(&mut e, 0, 1, Command::ReserveJob(reserved_id));
        let out = handle(&mut e, 0, 1, Command::Delete(reserved_id));
        assert_eq!(only(&out, 1), Response::Deleted);
    }

    #[test]
    fn delete_missing_or_reserved_by_other_is_not_found() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        e.connect(0, 2);
        let out = handle(&mut e, 0, 1, Command::Delete(42));
        assert_eq!(only(&out, 1), Response::NotFound);

        let id = put(&mut e, 0, 1, 0, 0, 0, "x");
        handle(&mut e, 0, 1, Command::ReserveJob(id));
        let out = handle(&mut e, 0, 2, Command::Delete(id));
        assert_eq!(only(&out, 2), Response::NotFound);
    }

    #[test]
    fn release_requeues_with_new_pri_and_delay() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        let id = put(&mut e, 0, 1, 5, 0, 0, "x");
        handle(&mut e, 0, 1, Command::ReserveJob(id));
        let out = handle(
            &mut e,
            0,
            1,
            Command::Release {
                id,
                pri: 77,
                delay: 10,
            },
        );
        assert_eq!(only(&out, 1), Response::Released);
        assert_eq!(e.t_job_state(id), Some("delayed"));
        assert_eq!(e.t_job_pri(id), Some(77));
    }

    #[test]
    fn release_with_zero_delay_wakes_a_waiter() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        e.connect(0, 2);
        let id = put(&mut e, 0, 1, 0, 0, 0, "x");
        handle(&mut e, 0, 1, Command::ReserveJob(id));
        let out = handle(&mut e, 0, 2, Command::Reserve);
        none_for(&out, 2);

        let out = handle(
            &mut e,
            0,
            1,
            Command::Release {
                id,
                pri: 0,
                delay: 0,
            },
        );
        assert_eq!(only(&out, 1), Response::Released);
        assert_eq!(
            only(&out, 2),
            Response::Reserved {
                id,
                body: Bytes::from_static(b"x")
            }
        );
    }

    #[test]
    fn release_not_found_when_not_reserved_by_caller() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        let out = handle(
            &mut e,
            0,
            1,
            Command::Release {
                id: 999,
                pri: 0,
                delay: 0,
            },
        );
        assert_eq!(only(&out, 1), Response::NotFound);
    }

    #[test]
    fn bury_moves_job_to_buried_and_sets_pri() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        let id = put(&mut e, 0, 1, 0, 0, 0, "x");
        handle(&mut e, 0, 1, Command::ReserveJob(id));
        let out = handle(&mut e, 0, 1, Command::Bury { id, pri: 3 });
        assert_eq!(only(&out, 1), Response::Buried);
        assert_eq!(e.t_job_state(id), Some("buried"));
        assert_eq!(e.t_job_pri(id), Some(3));
        assert_eq!(e.t_buried_ct(), 1);
    }

    #[test]
    fn bury_not_found_when_not_reserved_by_caller() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        let id = put(&mut e, 0, 1, 0, 0, 0, "x");
        let out = handle(&mut e, 0, 1, Command::Bury { id, pri: 0 });
        assert_eq!(only(&out, 1), Response::NotFound);
    }

    #[test]
    fn touch_resets_ttr_deadline() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        let id = put(&mut e, 0, 1, 0, 0, 4, "x");
        handle(&mut e, 0, 1, Command::ReserveJob(id));
        // Halfway through the TTR, touch should push the deadline forward
        // again so a tick at the *original* deadline no longer expires it.
        let out = handle(&mut e, 2 * SEC, 1, Command::Touch(id));
        assert_eq!(only(&out, 1), Response::Touched);

        let mut out = Outbox::new();
        e.tick(4 * SEC, &mut out);
        none_for(&out, 1);
        assert_eq!(e.t_job_state(id), Some("reserved"));
    }

    #[test]
    fn touch_not_found_when_not_reserved_by_caller() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        let out = handle(&mut e, 0, 1, Command::Touch(999));
        assert_eq!(only(&out, 1), Response::NotFound);
    }

    // -----------------------------------------------------------------
    // peek / peek-ready / peek-delayed / peek-buried
    // -----------------------------------------------------------------

    #[test]
    fn peek_variants_success_and_not_found() {
        let mut e = engine_at(0);
        e.connect(0, 1);

        let out = handle(&mut e, 0, 1, Command::PeekReady);
        assert_eq!(only(&out, 1), Response::NotFound);

        let id = put(&mut e, 0, 1, 0, 0, 0, "x");
        let out = handle(&mut e, 0, 1, Command::Peek(id));
        assert_eq!(
            only(&out, 1),
            Response::Found {
                id,
                body: Bytes::from_static(b"x")
            }
        );
        let out = handle(&mut e, 0, 1, Command::PeekReady);
        assert_eq!(
            only(&out, 1),
            Response::Found {
                id,
                body: Bytes::from_static(b"x")
            }
        );
        assert_eq!(
            only(&handle(&mut e, 0, 1, Command::PeekDelayed), 1),
            Response::NotFound
        );
        assert_eq!(
            only(&handle(&mut e, 0, 1, Command::PeekBuried), 1),
            Response::NotFound
        );

        handle(&mut e, 0, 1, Command::ReserveJob(id));
        handle(&mut e, 0, 1, Command::Bury { id, pri: 0 });
        let out = handle(&mut e, 0, 1, Command::PeekBuried);
        assert_eq!(
            only(&out, 1),
            Response::Found {
                id,
                body: Bytes::from_static(b"x")
            }
        );

        let out = handle(&mut e, 0, 1, Command::Peek(12345));
        assert_eq!(only(&out, 1), Response::NotFound);
    }

    #[test]
    fn peek_delayed_finds_delayed_job() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        let id = put(&mut e, 0, 1, 0, 50, 0, "x");
        let out = handle(&mut e, 0, 1, Command::PeekDelayed);
        assert_eq!(
            only(&out, 1),
            Response::Found {
                id,
                body: Bytes::from_static(b"x")
            }
        );
    }

    // -----------------------------------------------------------------
    // kick / kick-job
    // -----------------------------------------------------------------

    #[test]
    fn kick_prefers_buried_over_delayed() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        let delayed_id = put(&mut e, 0, 1, 0, 1000, 0, "d");
        let buried_id = put(&mut e, 0, 1, 0, 0, 0, "b");
        handle(&mut e, 0, 1, Command::ReserveJob(buried_id));
        handle(
            &mut e,
            0,
            1,
            Command::Bury {
                id: buried_id,
                pri: 0,
            },
        );

        let out = handle(&mut e, 0, 1, Command::Kick(5));
        assert_eq!(only(&out, 1), Response::Kicked(1));
        assert_eq!(e.t_job_state(buried_id), Some("ready"));
        // The delayed job was left untouched since a buried job existed.
        assert_eq!(e.t_job_state(delayed_id), Some("delayed"));
    }

    #[test]
    fn kick_falls_back_to_delayed_when_no_buried() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        let a = put(&mut e, 0, 1, 0, 1000, 0, "a");
        let b = put(&mut e, 0, 1, 0, 2000, 0, "b");
        let out = handle(&mut e, 0, 1, Command::Kick(1));
        assert_eq!(only(&out, 1), Response::Kicked(1));
        // Soonest-deadline delayed job (a) is kicked first.
        assert_eq!(e.t_job_state(a), Some("ready"));
        assert_eq!(e.t_job_state(b), Some("delayed"));
    }

    #[test]
    fn kick_bulk_is_bounded_by_n_and_counts_are_correct() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        for i in 0..3 {
            let id = put(&mut e, 0, 1, 0, 1000 + i, 0, "x");
            let _ = id;
        }
        let out = handle(&mut e, 0, 1, Command::Kick(2));
        assert_eq!(only(&out, 1), Response::Kicked(2));
        assert_eq!(e.t_tube_delayed_len(&tube("default")), Some(1));
    }

    #[test]
    fn kick_job_buried_and_delayed_succeed_others_not_found() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        let buried_id = put(&mut e, 0, 1, 0, 0, 0, "b");
        handle(&mut e, 0, 1, Command::ReserveJob(buried_id));
        handle(
            &mut e,
            0,
            1,
            Command::Bury {
                id: buried_id,
                pri: 0,
            },
        );
        let out = handle(&mut e, 0, 1, Command::KickJob(buried_id));
        assert_eq!(only(&out, 1), Response::KickedJob);
        assert_eq!(e.t_job_state(buried_id), Some("ready"));

        let delayed_id = put(&mut e, 0, 1, 0, 1000, 0, "d");
        let out = handle(&mut e, 0, 1, Command::KickJob(delayed_id));
        assert_eq!(only(&out, 1), Response::KickedJob);
        assert_eq!(e.t_job_state(delayed_id), Some("ready"));

        let ready_id = put(&mut e, 0, 1, 0, 0, 0, "r");
        let out = handle(&mut e, 0, 1, Command::KickJob(ready_id));
        assert_eq!(only(&out, 1), Response::NotFound);

        let out = handle(&mut e, 0, 1, Command::KickJob(999));
        assert_eq!(only(&out, 1), Response::NotFound);
    }

    // -----------------------------------------------------------------
    // pause-tube
    // -----------------------------------------------------------------

    #[test]
    fn pause_tube_not_found_for_missing_tube() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        let out = handle(
            &mut e,
            0,
            1,
            Command::PauseTube {
                tube: tube("nope"),
                delay: 5,
            },
        );
        assert_eq!(only(&out, 1), Response::NotFound);
    }

    #[test]
    fn pause_tube_blocks_dispatch_until_it_expires_via_tick() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        e.connect(0, 2);
        let out = handle(
            &mut e,
            0,
            1,
            Command::PauseTube {
                tube: tube("default"),
                delay: 10,
            },
        );
        assert_eq!(only(&out, 1), Response::Paused);

        // A job put while paused stays ready but undispatched.
        let id = put(&mut e, 0, 1, 0, 0, 0, "x");
        let out = handle(&mut e, 0, 2, Command::Reserve);
        none_for(&out, 2);
        assert!(e.t_conn_waiting(2));

        let stats = e.build_stats_tube(&tube("default"), 5 * SEC).unwrap();
        assert_eq!(stats.pause, 10);
        assert_eq!(stats.pause_time_left, 5);

        assert_eq!(e.next_deadline(), Some(10 * SEC));
        let mut out = Outbox::new();
        e.tick(10 * SEC, &mut out);
        assert_eq!(
            only(&out, 2),
            Response::Reserved {
                id,
                body: Bytes::from_static(b"x")
            }
        );
        assert!(!e.t_conn_waiting(2));
    }

    // -----------------------------------------------------------------
    // TTR expiry
    // -----------------------------------------------------------------

    #[test]
    fn ttr_expiry_returns_job_to_ready_and_counts_timeout() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        let id = put(&mut e, 0, 1, 0, 0, 2, "x");
        handle(&mut e, 0, 1, Command::ReserveJob(id));
        assert_eq!(e.next_deadline(), Some(2 * SEC));

        let mut out = Outbox::new();
        e.tick(2 * SEC + 1, &mut out);
        none_for(&out, 1);
        assert_eq!(e.t_job_state(id), Some("ready"));
        let stats = e.build_stats_job(id, 2 * SEC + 1).unwrap();
        assert_eq!(stats.timeouts, 1);
        assert_eq!(e.t_reserved_ct(), 0);
        assert_eq!(e.t_ready_ct(), 1);
    }

    #[test]
    fn ttr_expiry_wakes_a_different_waiting_connection() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        e.connect(0, 2);
        let id = put(&mut e, 0, 1, 0, 0, 1, "x");
        handle(&mut e, 0, 1, Command::ReserveJob(id));
        let out = handle(&mut e, 0, 2, Command::Reserve);
        none_for(&out, 2);

        let mut out = Outbox::new();
        e.tick(SEC + 1, &mut out);
        assert_eq!(
            only(&out, 2),
            Response::Reserved {
                id,
                body: Bytes::from_static(b"x")
            }
        );
    }

    // -----------------------------------------------------------------
    // multi-connection waiter wake order / multi-tube priority
    // -----------------------------------------------------------------

    /// Extracts the connection ids that received a `Reserved` reply, in the
    /// order those replies were pushed to the outbox (i.e. actual wake
    /// order), not in some other arbitrary order.
    fn reserved_order(out: &Outbox) -> Vec<u64> {
        out.iter()
            .filter(|(_, r)| matches!(r, Response::Reserved { .. }))
            .map(|(c, _)| *c)
            .collect()
    }

    #[test]
    fn odd_waiter_count_is_served_in_pure_arrival_order() {
        // With an odd number of waiting connections, sequential
        // single-job wakeups happen to come out in pure FIFO order (see
        // ms.c's comment on ms_take: the deviation only shows up for an
        // even count drained without intervening appends).
        let mut e = engine_at(0);
        for cid in 1..=3u64 {
            e.connect(0, cid);
            let out = handle(&mut e, 0, cid, Command::Reserve);
            none_for(&out, cid);
        }
        e.connect(0, 999);
        let mut woken = Vec::new();
        for _ in 0..3 {
            let out = handle(
                &mut e,
                0,
                999,
                Command::Put {
                    pri: 0,
                    delay: 0,
                    ttr: 0,
                    body: Bytes::from_static(b"x"),
                },
            );
            woken.extend(reserved_order(&out));
        }
        assert_eq!(woken, vec![1, 2, 3]);
    }

    #[test]
    fn even_waiter_count_deviates_from_arrival_order_via_ms_round_robin() {
        // Four waiting connections, served by four separate single-job
        // put/process_queue passes (no intervening appends to the tube's
        // waiting_conns set): this hits the same even-count exception as
        // ms::tests::take_even_count_deviates_from_fifo, and it must be
        // mirrored exactly because it decides which client gets which job.
        let mut e = engine_at(0);
        for cid in 1..=4u64 {
            e.connect(0, cid);
            let out = handle(&mut e, 0, cid, Command::Reserve);
            none_for(&out, cid);
        }
        e.connect(0, 999);
        let mut woken = Vec::new();
        for _ in 0..4 {
            let out = handle(
                &mut e,
                0,
                999,
                Command::Put {
                    pri: 0,
                    delay: 0,
                    ttr: 0,
                    body: Bytes::from_static(b"x"),
                },
            );
            woken.extend(reserved_order(&out));
        }
        assert_eq!(woken, vec![1, 2, 4, 3]);
    }

    #[test]
    fn draining_all_ready_jobs_at_once_matches_incremental_draining() {
        // Whether four already-ready jobs are handed out incrementally
        // (previous test) or all at once in a single process_queue pass
        // (triggered here by a pause expiring via tick()), the wake order
        // must be identical: it depends only on the sequence of
        // take()/append() calls against the tube's waiting_conns set, not
        // on how many top-level engine calls they are split across.
        let mut e = engine_at(0);
        e.connect(0, 999);
        handle(
            &mut e,
            0,
            999,
            Command::PauseTube {
                tube: tube("default"),
                delay: 10,
            },
        );
        for _ in 0..4 {
            put(&mut e, 0, 999, 0, 0, 0, "x");
        }
        for cid in 1..=4u64 {
            e.connect(0, cid);
            let out = handle(&mut e, 0, cid, Command::Reserve);
            none_for(&out, cid);
        }
        let mut out = Outbox::new();
        e.tick(10 * SEC, &mut out);
        assert_eq!(reserved_order(&out), vec![1, 2, 4, 3]);
    }

    #[test]
    fn reserve_picks_globally_best_priority_across_watched_tubes() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        e.connect(0, 2);
        handle(&mut e, 0, 2, Command::Use(tube("low")));
        let low_id = put(&mut e, 0, 2, 10, 0, 0, "low-pri-job");
        handle(&mut e, 0, 2, Command::Use(tube("high")));
        let high_id = put(&mut e, 0, 2, 1, 0, 0, "high-pri-job");
        let _ = low_id;

        handle(&mut e, 0, 1, Command::Watch(tube("low")));
        let out = handle(&mut e, 0, 1, Command::Watch(tube("high")));
        assert_eq!(only(&out, 1), Response::Watching(3));

        let out = handle(&mut e, 0, 1, Command::Reserve);
        assert_eq!(
            only(&out, 1),
            Response::Reserved {
                id: high_id,
                body: Bytes::from_static(b"high-pri-job")
            }
        );
    }

    // -----------------------------------------------------------------
    // disconnect / half-close
    // -----------------------------------------------------------------

    #[test]
    fn disconnect_releases_reserved_jobs_and_wakes_a_waiter() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        e.connect(0, 2);
        let id = put(&mut e, 0, 1, 0, 0, 0, "x");
        handle(&mut e, 0, 1, Command::ReserveJob(id));
        let out = handle(&mut e, 0, 2, Command::Reserve);
        none_for(&out, 2);

        let mut out = Outbox::new();
        e.disconnect(0, 1, &mut out);
        assert_eq!(
            only(&out, 2),
            Response::Reserved {
                id,
                body: Bytes::from_static(b"x")
            }
        );
        assert!(!e.t_conn_exists(1));
    }

    #[test]
    fn disconnect_removes_waiting_registration_cleanly() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        let out = handle(&mut e, 0, 1, Command::Reserve);
        none_for(&out, 1);
        assert_eq!(e.t_tube_waiting_conns(&tube("default")), Some(1));

        let mut out = Outbox::new();
        e.disconnect(0, 1, &mut out);
        assert!(out.is_empty());
        assert_eq!(e.t_tube_waiting_conns(&tube("default")), Some(0));
        assert_eq!(e.t_waiting_ct(), 0);
    }

    #[test]
    fn disconnect_dereferences_watched_and_used_tubes() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        handle(&mut e, 0, 1, Command::Use(tube("foo")));
        handle(&mut e, 0, 1, Command::Watch(tube("bar")));
        assert!(e.t_tube_exists(&tube("foo")));
        assert!(e.t_tube_exists(&tube("bar")));
        e.disconnect(0, 1, &mut Outbox::new());
        assert!(!e.t_tube_exists(&tube("foo")));
        assert!(!e.t_tube_exists(&tube("bar")));
    }

    #[test]
    fn half_close_while_waiting_returns_timed_out() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        let out = handle(&mut e, 0, 1, Command::Reserve);
        none_for(&out, 1);

        let mut out = Outbox::new();
        e.half_close(0, 1, &mut out);
        assert_eq!(only(&out, 1), Response::TimedOut);
        assert!(!e.t_conn_waiting(1));
    }

    #[test]
    fn half_close_while_not_waiting_is_a_noop() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        let mut out = Outbox::new();
        e.half_close(0, 1, &mut out);
        assert!(out.is_empty());
    }

    // -----------------------------------------------------------------
    // stats builders
    // -----------------------------------------------------------------

    #[test]
    fn build_stats_job_reports_expected_fields() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        let id = put(&mut e, 0, 1, 7, 0, 5, "hello");
        handle(&mut e, 0, 1, Command::ReserveJob(id));
        let s = e.build_stats_job(id, 3 * SEC).unwrap();
        assert_eq!(s.id, id);
        assert_eq!(s.tube, tube("default"));
        assert_eq!(s.state, "reserved");
        assert_eq!(s.pri, 7);
        assert_eq!(s.age, 3);
        assert_eq!(s.ttr, 5);
        assert_eq!(s.time_left, 2); // deadline=5s, now=3s
        assert_eq!(s.reserves, 1);
        assert_eq!(s.file, 0);
    }

    #[test]
    fn build_stats_job_none_for_missing_job() {
        let e = engine_at(0);
        assert!(e.build_stats_job(999, 0).is_none());
    }

    #[test]
    fn build_stats_tube_reports_counts() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        e.connect(0, 2);
        handle(&mut e, 0, 2, Command::Watch(tube("default")));
        let _r = put(&mut e, 0, 1, 0, 0, 0, "a");
        let _d = put(&mut e, 0, 1, 0, 100, 0, "b");
        let buried = put(&mut e, 0, 1, 0, 0, 0, "c");
        handle(&mut e, 0, 1, Command::ReserveJob(buried));
        handle(&mut e, 0, 1, Command::Bury { id: buried, pri: 0 });
        handle(&mut e, 0, 1, Command::Delete(_r));

        let s = e.build_stats_tube(&tube("default"), 0).unwrap();
        assert_eq!(s.current_jobs_ready, 0);
        assert_eq!(s.current_jobs_delayed, 1);
        assert_eq!(s.current_jobs_buried, 1);
        assert_eq!(s.cmd_delete, 1);
        // Both conn 1 and conn 2 use (and watch) "default" by default.
        assert_eq!(s.current_using, 2);
        assert_eq!(s.current_watching, 2);
    }

    #[test]
    fn build_stats_tube_none_for_missing_tube() {
        let e = engine_at(0);
        assert!(e.build_stats_tube(&tube("nope"), 0).is_none());
    }

    #[test]
    fn build_stats_server_reports_op_and_job_counters() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        let id = put(&mut e, 0, 1, 0, 0, 0, "x");
        handle(&mut e, 0, 1, Command::Stats);
        handle(&mut e, 0, 1, Command::ReserveJob(id));
        let s = e.build_stats_server(SEC);
        assert_eq!(s.cmd_put, 1);
        assert_eq!(s.cmd_stats, 1);
        assert_eq!(s.total_jobs, 1);
        assert_eq!(s.current_jobs_reserved, 1);
        assert_eq!(s.current_connections, 1);
        assert_eq!(s.current_producers, 1);
        assert_eq!(s.current_workers, 1);
        assert_eq!(s.uptime, 1);
        assert_eq!(s.max_job_size, bstk_proto::DEFAULT_MAX_JOB_SIZE as u64);
    }

    #[test]
    fn urgent_counter_only_tracks_ready_jobs_below_threshold() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        let id = put(&mut e, 0, 1, 10, 0, 0, "urgent");
        let s = e.build_stats_server(0);
        assert_eq!(s.current_jobs_urgent, 1);
        handle(&mut e, 0, 1, Command::ReserveJob(id));
        // Once reserved, it's no longer counted as "urgent" (matches
        // urgent_ct only being touched by the ready-queue helpers).
        let s = e.build_stats_server(0);
        assert_eq!(s.current_jobs_urgent, 0);
    }

    // -----------------------------------------------------------------
    // full Response::Ok path via to_yaml(); needs T1's implementation.
    // -----------------------------------------------------------------

    #[test]
    fn stats_command_full_response_needs_t1() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        let out = handle(&mut e, 0, 1, Command::Stats);
        match only(&out, 1) {
            Response::Ok(_) => {}
            other => panic!("expected Ok, got {other:?}"),
        }
    }

    #[test]
    fn stats_job_full_response_needs_t1() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        let id = put(&mut e, 0, 1, 0, 0, 0, "x");
        let out = handle(&mut e, 0, 1, Command::StatsJob(id));
        match only(&out, 1) {
            Response::Ok(_) => {}
            other => panic!("expected Ok, got {other:?}"),
        }
    }

    #[test]
    fn stats_tube_full_response_needs_t1() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        let out = handle(&mut e, 0, 1, Command::StatsTube(tube("default")));
        match only(&out, 1) {
            Response::Ok(_) => {}
            other => panic!("expected Ok, got {other:?}"),
        }
    }

    #[test]
    fn list_tubes_full_response_needs_t1() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        let out = handle(&mut e, 0, 1, Command::ListTubes);
        match only(&out, 1) {
            Response::Ok(_) => {}
            other => panic!("expected Ok, got {other:?}"),
        }
    }

    #[test]
    fn list_tubes_watched_full_response_needs_t1() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        let out = handle(&mut e, 0, 1, Command::ListTubesWatched);
        match only(&out, 1) {
            Response::Ok(_) => {}
            other => panic!("expected Ok, got {other:?}"),
        }
    }
}

// -----------------------------------------------------------------------
// Property-based state machine test.
//
// Drives random sequences of (conn, command, time advance) over a handful
// of connections and tubes, checking after *every* step that:
//   (a) each job belongs to exactly one container;
//   (b) every relevant counter equals the sum of the container sizes it's
//       supposed to track;
//   (c) every reserved job's reserver is a connection that still exists;
//   (d) no tube simultaneously has ready jobs and an unpaused waiting
//       connection (process_queue must always clear that situation).
// -----------------------------------------------------------------------
#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod proptests {
    use std::collections::HashSet;

    use bytes::Bytes;
    use proptest::prelude::*;

    use bstk_proto::{Command, JobId, TubeName};

    use crate::{ConnId, EngineConfig, Nanos, Outbox, StaticSysInfo};

    use super::Engine;

    const SEC: Nanos = crate::NANOS_PER_SEC;
    const TUBE_POOL: [&str; 3] = ["default", "a", "b"];

    fn tube_name(idx: usize) -> TubeName {
        TubeName::new(TUBE_POOL[idx % TUBE_POOL.len()]).expect("valid pool name")
    }

    #[derive(Debug, Clone)]
    enum Action {
        Connect(ConnId),
        Disconnect(ConnId),
        HalfClose(ConnId),
        Advance(u64),
        Put {
            conn: ConnId,
            pri: u32,
            delay: u32,
            ttr: u32,
        },
        Use {
            conn: ConnId,
            tube: usize,
        },
        Watch {
            conn: ConnId,
            tube: usize,
        },
        Ignore {
            conn: ConnId,
            tube: usize,
        },
        Reserve {
            conn: ConnId,
        },
        ReserveTimeout {
            conn: ConnId,
            timeout: u32,
        },
        ReserveJob {
            conn: ConnId,
            id: JobId,
        },
        Delete {
            conn: ConnId,
            id: JobId,
        },
        Release {
            conn: ConnId,
            id: JobId,
            pri: u32,
            delay: u32,
        },
        Bury {
            conn: ConnId,
            id: JobId,
            pri: u32,
        },
        Touch {
            conn: ConnId,
            id: JobId,
        },
        Kick {
            conn: ConnId,
            n: u32,
        },
        KickJob {
            conn: ConnId,
            id: JobId,
        },
        Pause {
            conn: ConnId,
            tube: usize,
            delay: u32,
        },
    }

    fn action_strategy() -> impl Strategy<Value = Action> {
        let conn = 0..5u64;
        let tube_idx = 0..TUBE_POOL.len();
        let job_id = 0..40u64;
        let pri = 0..2000u32;
        let small = 0..5u32;
        prop_oneof![
            conn.clone().prop_map(Action::Connect),
            conn.clone().prop_map(Action::Disconnect),
            conn.clone().prop_map(Action::HalfClose),
            (0..5u64).prop_map(Action::Advance),
            (conn.clone(), pri.clone(), small.clone(), small.clone()).prop_map(
                |(conn, pri, delay, ttr)| Action::Put {
                    conn,
                    pri,
                    delay,
                    ttr
                }
            ),
            (conn.clone(), tube_idx.clone()).prop_map(|(conn, tube)| Action::Use { conn, tube }),
            (conn.clone(), tube_idx.clone()).prop_map(|(conn, tube)| Action::Watch { conn, tube }),
            (conn.clone(), tube_idx.clone()).prop_map(|(conn, tube)| Action::Ignore { conn, tube }),
            conn.clone().prop_map(|conn| Action::Reserve { conn }),
            (conn.clone(), small.clone())
                .prop_map(|(conn, timeout)| Action::ReserveTimeout { conn, timeout }),
            (conn.clone(), job_id.clone()).prop_map(|(conn, id)| Action::ReserveJob { conn, id }),
            (conn.clone(), job_id.clone()).prop_map(|(conn, id)| Action::Delete { conn, id }),
            (conn.clone(), job_id.clone(), pri.clone(), small.clone()).prop_map(
                |(conn, id, pri, delay)| Action::Release {
                    conn,
                    id,
                    pri,
                    delay
                }
            ),
            (conn.clone(), job_id.clone(), pri.clone()).prop_map(|(conn, id, pri)| Action::Bury {
                conn,
                id,
                pri
            }),
            (conn.clone(), job_id.clone()).prop_map(|(conn, id)| Action::Touch { conn, id }),
            (conn.clone(), small.clone()).prop_map(|(conn, n)| Action::Kick { conn, n }),
            (conn.clone(), job_id.clone()).prop_map(|(conn, id)| Action::KickJob { conn, id }),
            (conn, tube_idx, small).prop_map(|(conn, tube, delay)| Action::Pause {
                conn,
                tube,
                delay
            }),
        ]
    }

    fn body() -> Bytes {
        Bytes::from_static(b"x")
    }

    fn check_invariants(e: &Engine) {
        let all_ids: HashSet<JobId> = e.t_all_job_ids().into_iter().collect();
        let tubes = e.t_all_tube_names();
        let conns = e.t_all_conn_ids();

        // (a) each job belongs to exactly one container.
        let mut seen: std::collections::HashMap<JobId, u32> = std::collections::HashMap::new();
        for t in &tubes {
            for id in e.t_tube_ready_ids(t) {
                assert_eq!(e.t_job_state(id), Some("ready"));
                *seen.entry(id).or_insert(0) += 1;
            }
            for id in e.t_tube_delayed_ids(t) {
                assert_eq!(e.t_job_state(id), Some("delayed"));
                *seen.entry(id).or_insert(0) += 1;
            }
            for id in e.t_tube_buried_ids(t) {
                assert_eq!(e.t_job_state(id), Some("buried"));
                *seen.entry(id).or_insert(0) += 1;
            }
        }
        for &c in &conns {
            for id in e.t_conn_reserved(c) {
                assert_eq!(e.t_job_state(id), Some("reserved"));
                assert_eq!(e.t_job_reserver(id), Some(c));
                *seen.entry(id).or_insert(0) += 1;
            }
        }
        for (&id, &count) in &seen {
            assert_eq!(count, 1, "job {id} appears in {count} containers");
        }
        assert_eq!(
            seen.keys().copied().collect::<HashSet<_>>(),
            all_ids,
            "container membership doesn't match the job table"
        );

        // (b) counters equal container sizes.
        let ready_total: u64 = tubes
            .iter()
            .map(|t| e.t_tube_ready_len(t).unwrap_or(0) as u64)
            .sum();
        assert_eq!(e.t_ready_ct(), ready_total, "ready_ct mismatch");
        let reserved_total: u64 = conns
            .iter()
            .map(|c| e.t_conn_reserved(*c).len() as u64)
            .sum();
        assert_eq!(e.t_reserved_ct(), reserved_total, "reserved_ct mismatch");
        let buried_total: u64 = tubes
            .iter()
            .map(|t| e.t_tube_buried_len(t).unwrap_or(0) as u64)
            .sum();
        assert_eq!(e.t_buried_ct(), buried_total, "buried_ct mismatch");
        let waiting_total: u64 = conns.iter().filter(|c| e.t_conn_waiting(**c)).count() as u64;
        assert_eq!(e.t_waiting_ct(), waiting_total, "waiting_ct mismatch");

        // (c) every reserved job's reserver is a live connection.
        for &id in &all_ids {
            if e.t_job_state(id) == Some("reserved") {
                let reserver = e.t_job_reserver(id).expect("reserved job has a reserver");
                assert!(
                    e.t_conn_exists(reserver),
                    "job {id} reserved by dead conn {reserver}"
                );
            }
        }

        // (d) no tube has ready jobs while it also has an unpaused waiting conn.
        for t in &tubes {
            let ready_len = e.t_tube_ready_len(t).unwrap_or(0);
            let waiting_len = e.t_tube_waiting_conns(t).unwrap_or(0);
            let paused = e.t_tube_paused(t).unwrap_or(false);
            if ready_len > 0 && waiting_len > 0 {
                assert!(
                    paused,
                    "tube {t:?} has {ready_len} ready job(s) and {waiting_len} waiting conn(s) but isn't paused"
                );
            }
        }
    }

    fn run_actions(actions: Vec<Action>) {
        let mut e = Engine::new(
            0,
            EngineConfig::default(),
            Box::new(StaticSysInfo::default()),
        );
        let mut now: Nanos = 0;
        let mut ever_connected: HashSet<ConnId> = HashSet::new();
        let mut out = Outbox::new();

        check_invariants(&e);
        for action in actions {
            out.clear();
            // The interface contract says a caller must not send another
            // command for a connection until it has received the reply to
            // the previous one; a connection blocked in `reserve` has no
            // reply yet. Connect/disconnect/half-close/tick are not wire
            // commands and remain valid regardless.
            let cmd: Option<(ConnId, Command)> = match action {
                Action::Connect(c) => {
                    if ever_connected.insert(c) {
                        e.connect(now, c);
                    }
                    None
                }
                Action::Disconnect(c) => {
                    e.disconnect(now, c, &mut out);
                    None
                }
                Action::HalfClose(c) => {
                    e.half_close(now, c, &mut out);
                    None
                }
                Action::Advance(dt) => {
                    now += dt * SEC;
                    e.tick(now, &mut out);
                    None
                }
                Action::Put {
                    conn,
                    pri,
                    delay,
                    ttr,
                } => Some((
                    conn,
                    Command::Put {
                        pri,
                        delay,
                        ttr,
                        body: body(),
                    },
                )),
                Action::Use { conn, tube } => Some((conn, Command::Use(tube_name(tube)))),
                Action::Watch { conn, tube } => Some((conn, Command::Watch(tube_name(tube)))),
                Action::Ignore { conn, tube } => Some((conn, Command::Ignore(tube_name(tube)))),
                Action::Reserve { conn } => Some((conn, Command::Reserve)),
                Action::ReserveTimeout { conn, timeout } => {
                    Some((conn, Command::ReserveWithTimeout(timeout)))
                }
                Action::ReserveJob { conn, id } => Some((conn, Command::ReserveJob(id))),
                Action::Delete { conn, id } => Some((conn, Command::Delete(id))),
                Action::Release {
                    conn,
                    id,
                    pri,
                    delay,
                } => Some((conn, Command::Release { id, pri, delay })),
                Action::Bury { conn, id, pri } => Some((conn, Command::Bury { id, pri })),
                Action::Touch { conn, id } => Some((conn, Command::Touch(id))),
                Action::Kick { conn, n } => Some((conn, Command::Kick(n))),
                Action::KickJob { conn, id } => Some((conn, Command::KickJob(id))),
                Action::Pause { conn, tube, delay } => Some((
                    conn,
                    Command::PauseTube {
                        tube: tube_name(tube),
                        delay,
                    },
                )),
            };
            if let Some((conn, cmd)) = cmd
                && !e.t_conn_waiting(conn)
            {
                e.handle(now, conn, cmd, &mut out);
            }
            check_invariants(&e);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 10_000, ..ProptestConfig::default() })]

        #[test]
        fn state_machine_invariants_hold(actions in prop::collection::vec(action_strategy(), 15..35)) {
            run_actions(actions);
        }
    }
}
