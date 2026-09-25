//! The deterministic beanstalkd state machine. Ground truth for every rule
//! implemented here is `.ref/beanstalkd/{prot,conn,tube,job,ms,heap}.c`.

use std::collections::HashMap;

use bytes::Bytes;

use bstk_proto::{
    Command, JobId, PutRejection, Response, StatsJob, StatsServer, StatsTube, TubeName,
    URGENT_THRESHOLD,
};

use crate::model::{ConnState, JobRec, JobState, PendingPut, TubeState};
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

        // `ms_clear(&c->watch)` deletes index 0 repeatedly (swap with last),
        // dropping each tube's reference in that order. Tube destruction
        // order decides the survivors' order in the global tube list.
        let watched: Vec<TubeName> = self
            .conns
            .get_mut(&conn)
            .map(|c| c.watch.clear_in_delete_order())
            .unwrap_or_default();
        for t in watched {
            if let Some(ts) = self.tubes.get_mut(&t) {
                ts.watching_ct = ts.watching_ct.saturating_sub(1);
            }
            self.gc_tube_if_orphan(&t);
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

    /// A `put` command line was accepted and its body is about to be read
    /// (`Frame::PutStarted`). Applies prot.c's header-time side effects:
    /// `op_ct[OP_PUT]++`, and unless `too_big`, `connsetproducer` plus
    /// `make_job` (allocating the job id). No reply. The put's completion
    /// (`handle(Put)` or `put_rejected`) then skips those side effects.
    pub fn put_started(&mut self, now: Nanos, conn: ConnId, too_big: bool) {
        if !self.conns.contains_key(&conn) {
            return;
        }
        self.cmd_put += 1;
        let id = if too_big {
            None
        } else {
            self.connsetproducer(conn);
            let id = self.next_job_id;
            self.next_job_id += 1;
            Some(id)
        };
        if let Some(c) = self.conns.get_mut(&conn) {
            c.pending_put = Some(PendingPut {
                id,
                created_at: now,
            });
        }
    }

    /// A `put` rejected by the codec. Mirrors the side effects prot.c
    /// performs before each rejection: `op_ct[OP_PUT]++` happens right after
    /// the numeric fields parse, and the EXPECTED_CRLF path has already run
    /// `connsetproducer` and `make_job` (which consumes a job id). If
    /// `put_started` already applied them for this put, they are not
    /// repeated.
    pub fn put_rejected(&mut self, _now: Nanos, conn: ConnId, why: PutRejection, out: &mut Outbox) {
        let Some(c) = self.conns.get_mut(&conn) else {
            return;
        };
        let started = c.pending_put.take().is_some();
        if !started {
            self.cmd_put += 1;
            if why == PutRejection::ExpectedCrlf {
                self.connsetproducer(conn);
                self.next_job_id += 1;
            }
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
            Command::PauseTubeBadName => {
                // prot.c: op_ct[OP_PAUSE_TUBE]++ precedes is_valid_tube().
                self.cmd_pause_tube += 1;
                out.push((conn, Response::BadFormat));
            }
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
        let ttr = ttr.max(1);

        // The reference always allocates a job id (bumping next_id) before
        // checking drain mode, even though a draining put discards the job
        // afterwards. We mirror that: the id is consumed either way. When
        // `put_started` ran for this put, that already happened at header
        // time (as in prot.c), so reuse its id.
        let pending = self.conns.get_mut(&cid).and_then(|c| c.pending_put.take());
        let (id, created_at) = match pending {
            Some(PendingPut {
                id: Some(id),
                created_at,
            }) => (id, created_at),
            _ => {
                self.cmd_put += 1;
                self.connsetproducer(cid);
                let id = self.next_job_id;
                self.next_job_id += 1;
                (id, now)
            }
        };

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
            created_at,
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
            // The reference keeps waiting until the next prottick, whose
            // conn_timeout checks "deadline soon" before the explicit
            // timeout. That is true here when the ready job that skipped the
            // DEADLINE_SOON shortcut above could not be handed to this
            // connection: it sits in a paused tube, or process_queue gave it
            // to another waiter ahead in ms_take order.
            let soon = self.conn_deadline_soon(cid, now);
            self.do_remove_waiting_conn(cid);
            let reply = if soon {
                Response::DeadlineSoon
            } else {
                Response::TimedOut
            };
            out.push((cid, reply));
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
        // prot.c: `if (delay == 0) delay = 1;` runs on the delay already
        // converted to nanoseconds, so "pause 0" pauses for 1 ns (which
        // `stats-tube` reports as `pause: 0`), not for 1 second.
        let delay_nanos = if delay == 0 {
            1
        } else {
            (delay as Nanos) * NANOS_PER_SEC
        };
        if let Some(t) = self.tubes.get_mut(&tube) {
            t.pause = delay_nanos;
            t.unpause_at = now + delay_nanos;
            t.stat.pause_ct += 1;
        }
        out.push((cid, Response::Paused));
    }
}

// ---------------------------------------------------------------------
// Stats / introspection builders. Public in the oracle copy only, so the
// differential proptest in bstk-engine can compare stats structs directly.
// ---------------------------------------------------------------------
impl Engine {
    pub fn tube_names(&self) -> Vec<TubeName> {
        self.tube_order.items.clone()
    }

    pub fn watched_tube_names(&self, cid: ConnId) -> Vec<TubeName> {
        self.conns
            .get(&cid)
            .map(|c| c.watch.items.clone())
            .unwrap_or_default()
    }

    pub fn build_stats_job(&self, id: JobId, now: Nanos) -> Option<StatsJob> {
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

    pub fn build_stats_tube(&self, name: &TubeName, now: Nanos) -> Option<StatsTube> {
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

    pub fn build_stats_server(&self, now: Nanos) -> StatsServer {
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
