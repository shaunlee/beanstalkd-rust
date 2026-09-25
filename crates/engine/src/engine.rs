//! The deterministic beanstalkd state machine. Ground truth for every rule
//! implemented here is `.ref/beanstalkd/{prot,conn,tube,job,ms,heap}.c`.
//!
//! Every time-driven event is kept in an ordered index, so `tick` and
//! `next_deadline` cost O(log n) instead of scanning every tube and
//! connection:
//!
//! * `conn_ticks`: each connection's `conntickat` (the earliest of its
//!   reserve timeout, the safety margin of its soonest reserved job, and
//!   that job's TTR expiry), like the reference's connection heap.
//! * `delay_heads`: the deadline of the soonest delayed job of each tube.
//! * `pauses`: the unpause time of each paused tube.
//!
//! `dispatchable` holds the tubes that have both waiting connections and
//! ready jobs, the only ones `process_queue` can act on. Each index is
//! updated by the helper that changes its inputs (`refresh_conn_tick`,
//! `refresh_delay_head`, `refresh_dispatchable`, `set_pause` /
//! `clear_expired_pauses`). The frozen pre-index engine in
//! `crates/engine-oracle` is the behavioral reference for all of this (see
//! the `oracle` test module).

use std::collections::{BTreeSet, HashMap, HashSet};

use bytes::Bytes;

use bstk_proto::{
    Command, JobId, PutRejection, Response, StatsJob, StatsServer, StatsTube, TubeName,
    URGENT_THRESHOLD,
};

use crate::model::{ConnState, JobRec, JobState, PendingPut, TubeId, TubeState};
use crate::ms::Ms;
use crate::{
    BinlogStats, ConnId, EngineConfig, EngineInput, EngineState, JobRecord, JournalEntry,
    NANOS_PER_SEC, Nanos, Outbox, RecordState, Recovery, StateError, SysInfo,
};

/// `SAFETY_MARGIN` in conn.c: 1 second.
const SAFETY_MARGIN: Nanos = NANOS_PER_SEC;

/// "default" is created first and never destroyed, so it always has id 0.
const DEFAULT_TUBE: TubeId = 0;

pub struct Engine {
    cfg: EngineConfig,
    sys: Box<dyn SysInfo>,
    start: Nanos,
    draining: bool,

    next_job_id: JobId,
    jobs: HashMap<JobId, JobRec>,

    /// Tube slab, indexed by `TubeId`; `None` marks a free slot.
    tubes: Vec<Option<TubeState>>,
    free_tube_ids: Vec<TubeId>,
    tube_ids: HashMap<TubeName, TubeId>,
    /// Mirrors the reference's global `tubes` `Ms` array: insertion order,
    /// with swap-removal on GC. Drives list-tubes output order and the
    /// tie-break between equal delayed-job deadlines.
    tube_order: Ms<TubeId>,

    conns: HashMap<ConnId, ConnState>,

    /// `(conntickat, conn)` for every connection that has one.
    conn_ticks: BTreeSet<(Nanos, ConnId)>,
    /// `(deadline of the soonest delayed job, tube)` for every tube with
    /// delayed jobs.
    delay_heads: BTreeSet<(Nanos, TubeId)>,
    /// `(unpause_at, tube)` for every tube with `pause > 0`.
    pauses: BTreeSet<(Nanos, TubeId)>,
    /// Tubes with both waiting connections and ready jobs.
    dispatchable: BTreeSet<TubeId>,

    // Global counters (see `struct stats global_stat` and friends in dat.h).
    ready_ct: u64,
    urgent_ct: u64,
    reserved_ct: u64,
    buried_ct: u64,
    delayed_ct: u64,
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

    /// Pending binlog records (only ever non-empty when `cfg.journal`).
    journal: Vec<JournalEntry>,
    /// Binlog fields of `stats`, pushed by the server.
    binlog: BinlogStats,
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
            tubes: Vec::new(),
            free_tube_ids: Vec::new(),
            tube_ids: HashMap::new(),
            tube_order: Ms::new(),
            conns: HashMap::new(),
            conn_ticks: BTreeSet::new(),
            delay_heads: BTreeSet::new(),
            pauses: BTreeSet::new(),
            dispatchable: BTreeSet::new(),
            ready_ct: 0,
            urgent_ct: 0,
            reserved_ct: 0,
            buried_ct: 0,
            delayed_ct: 0,
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
            journal: Vec::new(),
            binlog: BinlogStats::default(),
        };
        // The "default" tube is immortal (see TubeState::refs / gc_tube_if_orphan).
        let default = e.find_or_make_tube(&TubeName::default_tube());
        debug_assert_eq!(default, DEFAULT_TUBE);
        e
    }

    pub fn connect(&mut self, _now: Nanos, conn: ConnId) {
        if let Some(t) = self.tube_mut(DEFAULT_TUBE) {
            t.using_ct += 1;
            t.watching_ct += 1;
        }
        if let Some(old) = self.conns.insert(conn, ConnState::new(DEFAULT_TUBE))
            && let Some(k) = old.tick_key
        {
            // Connection ids are never reused; keep the index consistent
            // anyway.
            self.conn_ticks.remove(&(k, conn));
        }
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
            if let Some(tube) = self.jobs.get(&job_id).map(|j| j.tube) {
                self.insert_ready(tube, job_id);
            }
            self.process_queue(now, out);
        }

        // `ms_clear(&c->watch)` deletes index 0 repeatedly (swap with last),
        // dropping each tube's reference in that order. Tube destruction
        // order decides the survivors' order in the global tube list.
        let watched: Vec<TubeId> = self
            .conns
            .get_mut(&conn)
            .map(|c| c.watch.clear_in_delete_order())
            .unwrap_or_default();
        for t in watched {
            if let Some(ts) = self.tube_mut(t) {
                ts.watching_ct = ts.watching_ct.saturating_sub(1);
            }
            self.gc_tube_if_orphan(t);
        }

        if let Some(use_tube) = self.conns.get(&conn).map(|c| c.use_tube) {
            if let Some(ts) = self.tube_mut(use_tube) {
                ts.using_ct = ts.using_ct.saturating_sub(1);
            }
            self.gc_tube_if_orphan(use_tube);
        }

        if let Some(c) = self.conns.remove(&conn) {
            if let Some(k) = c.tick_key {
                self.conn_ticks.remove(&(k, conn));
            }
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
            // Both paths run after `connsetproducer` and `make_job`.
            if matches!(why, PutRejection::ExpectedCrlf | PutRejection::OutOfMemory) {
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
        // Nothing is due before the earliest indexed deadline. This is the
        // common case: the server ticks after every message.
        match self.next_deadline() {
            Some(d) if d <= now => {}
            _ => return,
        }

        // 1. Delayed jobs whose deadline has passed, soonest first.
        while let Some((deadline, tube, id)) = self.soonest_delayed_job() {
            if deadline > now {
                break;
            }
            self.remove_delayed(tube, id);
            self.insert_ready(tube, id);
            self.process_queue(now, out);
        }

        // 2. Tube pauses whose expiry has passed. `process_queue` clears
        // every expired pause before dispatching (as the reference's
        // `next_awaited_job` does), so one call covers all of them.
        if self.pauses.first().is_some_and(|&(at, _)| at <= now) {
            self.process_queue(now, out);
        }

        // 3. Connections with a due TTR/margin/explicit-timeout event,
        // processed one at a time in `(tickat, conn)` order (re-reading the
        // index each round, since processing one connection can change
        // others' schedules via process_queue reassignment). Each
        // connection is handled at most once per tick() call:
        // `conn_timeout` drains every one of *its* overdue reserved jobs and
        // reaches a final, stable decision, so a second pass over the same
        // still-due connection (e.g. an exact boundary case where the
        // deadline equals `now`, mirroring the reference's strict `>=`
        // check, which yields a genuine no-op) cannot make further progress
        // and must not be retried.
        let mut processed: HashSet<ConnId> = HashSet::new();
        loop {
            let next = self
                .conn_ticks
                .iter()
                .take_while(|&&(t, _)| t <= now)
                .find(|&&(_, cid)| !processed.contains(&cid))
                .map(|&(_, cid)| cid);
            let Some(cid) = next else { break };
            processed.insert(cid);
            self.conn_timeout(cid, now, out);
        }
    }

    pub fn next_deadline(&self) -> Option<Nanos> {
        [
            self.delay_heads.first().map(|&(d, _)| d),
            self.pauses.first().map(|&(d, _)| d),
            self.conn_ticks.first().map(|&(d, _)| d),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    /// P3: run one input, then `tick(now)` (see `EngineInput`).
    pub fn apply_input(&mut self, now: Nanos, input: EngineInput, out: &mut Outbox) {
        match input {
            EngineInput::Connect(c) => self.connect(now, c),
            EngineInput::Disconnect(c) => self.disconnect(now, c, out),
            EngineInput::HalfClose(c) => self.half_close(now, c, out),
            EngineInput::PutStarted { conn, too_big } => self.put_started(now, conn, too_big),
            EngineInput::PutRejected { conn, why } => self.put_rejected(now, conn, why, out),
            EngineInput::Command { conn, cmd } => self.handle(now, conn, cmd, out),
            EngineInput::Tick => {}
            EngineInput::SetDraining(on) => self.set_draining(on),
        }
        self.tick(now, out);
    }

    /// P3: ids of all connections, ascending.
    pub fn conn_ids(&self) -> Vec<ConnId> {
        let mut ids: Vec<ConnId> = self.conns.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    pub fn config(&self) -> &EngineConfig {
        &self.cfg
    }

    /// P3: full state for a snapshot (everything but `sys` and the pending
    /// journal). No side effects. Maps are exported sorted by key, so equal
    /// engines export equal states (and identical serialized bytes).
    pub fn export_state(&self) -> EngineState {
        // Exhaustive destructuring: a new `Engine` field fails to compile
        // here until it is added to `EngineState`.
        let Engine {
            cfg,
            sys: _,
            start,
            draining,
            next_job_id,
            jobs,
            tubes,
            free_tube_ids,
            tube_ids,
            tube_order,
            conns,
            conn_ticks,
            delay_heads,
            pauses,
            dispatchable,
            ready_ct,
            urgent_ct,
            reserved_ct,
            buried_ct,
            delayed_ct,
            waiting_ct,
            total_jobs_ct,
            timeout_ct,
            cur_conns,
            tot_conns,
            cur_producers,
            cur_workers,
            cmd_put,
            cmd_peek,
            cmd_peek_ready,
            cmd_peek_delayed,
            cmd_peek_buried,
            cmd_reserve,
            cmd_reserve_with_timeout,
            cmd_delete,
            cmd_release,
            cmd_use,
            cmd_watch,
            cmd_ignore,
            cmd_bury,
            cmd_kick,
            cmd_touch,
            cmd_stats,
            cmd_stats_job,
            cmd_stats_tube,
            cmd_list_tubes,
            cmd_list_tube_used,
            cmd_list_tubes_watched,
            cmd_pause_tube,
            journal: _,
            binlog,
        } = self;

        let mut jobs: Vec<JobRec> = jobs.values().cloned().collect();
        jobs.sort_unstable_by_key(|j| j.id);
        let mut tube_ids: Vec<(TubeName, TubeId)> =
            tube_ids.iter().map(|(n, &id)| (n.clone(), id)).collect();
        tube_ids.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let mut conns: Vec<(ConnId, ConnState)> =
            conns.iter().map(|(&id, c)| (id, c.clone())).collect();
        conns.sort_unstable_by_key(|&(id, _)| id);

        EngineState {
            cfg: cfg.clone(),
            start: *start,
            draining: *draining,
            next_job_id: *next_job_id,
            jobs,
            tubes: tubes.clone(),
            free_tube_ids: free_tube_ids.clone(),
            tube_ids,
            tube_order: tube_order.clone(),
            conns,
            conn_ticks: conn_ticks.clone(),
            delay_heads: delay_heads.clone(),
            pauses: pauses.clone(),
            dispatchable: dispatchable.clone(),
            ready_ct: *ready_ct,
            urgent_ct: *urgent_ct,
            reserved_ct: *reserved_ct,
            buried_ct: *buried_ct,
            delayed_ct: *delayed_ct,
            waiting_ct: *waiting_ct,
            total_jobs_ct: *total_jobs_ct,
            timeout_ct: *timeout_ct,
            cur_conns: *cur_conns,
            tot_conns: *tot_conns,
            cur_producers: *cur_producers,
            cur_workers: *cur_workers,
            cmd_put: *cmd_put,
            cmd_peek: *cmd_peek,
            cmd_peek_ready: *cmd_peek_ready,
            cmd_peek_delayed: *cmd_peek_delayed,
            cmd_peek_buried: *cmd_peek_buried,
            cmd_reserve: *cmd_reserve,
            cmd_reserve_with_timeout: *cmd_reserve_with_timeout,
            cmd_delete: *cmd_delete,
            cmd_release: *cmd_release,
            cmd_use: *cmd_use,
            cmd_watch: *cmd_watch,
            cmd_ignore: *cmd_ignore,
            cmd_bury: *cmd_bury,
            cmd_kick: *cmd_kick,
            cmd_touch: *cmd_touch,
            cmd_stats: *cmd_stats,
            cmd_stats_job: *cmd_stats_job,
            cmd_stats_tube: *cmd_stats_tube,
            cmd_list_tubes: *cmd_list_tubes,
            cmd_list_tube_used: *cmd_list_tube_used,
            cmd_list_tubes_watched: *cmd_list_tubes_watched,
            cmd_pause_tube: *cmd_pause_tube,
            binlog: *binlog,
        }
    }

    /// P3: rebuild from a snapshot. The state comes from disk or from a
    /// peer, so it is untrusted: every invariant the engine relies on is
    /// checked (see `validate`) and a violation is an error, never a panic.
    /// The journal starts empty.
    pub fn import_state(state: EngineState, sys: Box<dyn SysInfo>) -> Result<Engine, StateError> {
        let EngineState {
            cfg,
            start,
            draining,
            next_job_id,
            jobs: job_list,
            tubes,
            free_tube_ids,
            tube_ids: tube_id_list,
            tube_order,
            conns: conn_list,
            conn_ticks,
            delay_heads,
            pauses,
            dispatchable,
            ready_ct,
            urgent_ct,
            reserved_ct,
            buried_ct,
            delayed_ct,
            waiting_ct,
            total_jobs_ct,
            timeout_ct,
            cur_conns,
            tot_conns,
            cur_producers,
            cur_workers,
            cmd_put,
            cmd_peek,
            cmd_peek_ready,
            cmd_peek_delayed,
            cmd_peek_buried,
            cmd_reserve,
            cmd_reserve_with_timeout,
            cmd_delete,
            cmd_release,
            cmd_use,
            cmd_watch,
            cmd_ignore,
            cmd_bury,
            cmd_kick,
            cmd_touch,
            cmd_stats,
            cmd_stats_job,
            cmd_stats_tube,
            cmd_list_tubes,
            cmd_list_tube_used,
            cmd_list_tubes_watched,
            cmd_pause_tube,
            binlog,
        } = state;

        // The sorted-vector encodings must be strictly ascending (canonical,
        // no duplicate keys).
        let err = |m: String| StateError(m);
        let mut jobs: HashMap<JobId, JobRec> = HashMap::with_capacity(job_list.len());
        let mut prev: Option<JobId> = None;
        for j in job_list {
            if prev.is_some_and(|p| j.id <= p) {
                return Err(err(format!("jobs not strictly ascending at id {}", j.id)));
            }
            prev = Some(j.id);
            jobs.insert(j.id, j);
        }
        let mut tube_ids: HashMap<TubeName, TubeId> = HashMap::with_capacity(tube_id_list.len());
        let mut prev: Option<TubeName> = None;
        for (name, id) in tube_id_list {
            if prev.as_ref().is_some_and(|p| name <= *p) {
                return Err(err(format!("tube_ids not strictly ascending at {name}")));
            }
            prev = Some(name.clone());
            tube_ids.insert(name, id);
        }
        let mut conns: HashMap<ConnId, ConnState> = HashMap::with_capacity(conn_list.len());
        let mut prev: Option<ConnId> = None;
        for (id, c) in conn_list {
            if prev.is_some_and(|p| id <= p) {
                return Err(err(format!("conns not strictly ascending at id {id}")));
            }
            prev = Some(id);
            conns.insert(id, c);
        }

        let e = Engine {
            cfg,
            sys,
            start,
            draining,
            next_job_id,
            jobs,
            tubes,
            free_tube_ids,
            tube_ids,
            tube_order,
            conns,
            conn_ticks,
            delay_heads,
            pauses,
            dispatchable,
            ready_ct,
            urgent_ct,
            reserved_ct,
            buried_ct,
            delayed_ct,
            waiting_ct,
            total_jobs_ct,
            timeout_ct,
            cur_conns,
            tot_conns,
            cur_producers,
            cur_workers,
            cmd_put,
            cmd_peek,
            cmd_peek_ready,
            cmd_peek_delayed,
            cmd_peek_buried,
            cmd_reserve,
            cmd_reserve_with_timeout,
            cmd_delete,
            cmd_release,
            cmd_use,
            cmd_watch,
            cmd_ignore,
            cmd_bury,
            cmd_kick,
            cmd_touch,
            cmd_stats,
            cmd_stats_job,
            cmd_stats_tube,
            cmd_list_tubes,
            cmd_list_tube_used,
            cmd_list_tubes_watched,
            cmd_pause_tube,
            journal: Vec::new(),
            binlog,
        };
        e.validate().map_err(err)?;
        Ok(e)
    }

    pub fn set_draining(&mut self, on: bool) {
        self.draining = on;
    }

    /// Rebuilds the state after a restart, as `prot_replay` in prot.c does
    /// for the job list `walinit` read (docs/PLAN.md §4.1):
    ///
    /// * jobs are created in the given (first-record) order, so tubes are
    ///   created in order of first appearance after "default", and buried
    ///   jobs enter each tube's buried FIFO in that order;
    /// * a buried job goes through `bury_job` again, which counts one more
    ///   bury (the reference's replay quirk);
    /// * a delayed job whose deadline has passed (`deadline_at <= now`)
    ///   becomes ready and keeps its `delay`; otherwise it stays delayed
    ///   until its original deadline;
    /// * cumulative counters (`cmd-*`, `total-jobs`, per-tube `total-jobs`
    ///   and `cmd-delete`, ...) start at zero, and recovered jobs don't count
    ///   toward `total-jobs`;
    /// * no journal entries are produced.
    ///
    /// A job reserved at crash time was never journaled as reserved, so it
    /// arrives here in its last journaled state, with that record's
    /// counters. (A reserved state never appears in a record; in the
    /// reference only compaction writes one, and `readrec` maps it to
    /// ready.)
    ///
    /// Known difference: while replaying, the reference also creates the
    /// tube of a deleted job at its put record and destroys it (swap-remove)
    /// at its delete record, which can reorder `list-tubes`. `Recovery` only
    /// carries live jobs, so we cannot reproduce that (docs/COMPAT.md).
    pub fn recover(
        now: Nanos,
        cfg: EngineConfig,
        sys: Box<dyn SysInfo>,
        recovery: Recovery,
    ) -> Self {
        let mut e = Engine::new(now, cfg, sys);
        // Create tubes in the reference's post-replay list order first
        // (`Recovery::tube_order`), but only tubes that will hold a job, so
        // no unreferenced tube is left behind.
        let live_tubes: std::collections::HashSet<&TubeName> =
            recovery.jobs.iter().map(|rj| &rj.tube).collect();
        for name in &recovery.tube_order {
            if live_tubes.contains(name) {
                e.find_or_make_tube(name);
            }
        }
        let mut max_id: JobId = 0;
        for rj in recovery.jobs {
            let r = rj.record;
            // Ids are unique in a well-formed recovery; keep the first.
            if e.jobs.contains_key(&r.id) {
                continue;
            }
            max_id = max_id.max(r.id);
            let tube = e.find_or_make_tube(&rj.tube);
            e.jobs.insert(
                r.id,
                JobRec {
                    id: r.id,
                    tube,
                    pri: r.pri,
                    delay: r.delay,
                    ttr: r.ttr.max(1),
                    body: rj.body,
                    created_at: r.created_at,
                    deadline_at: 0,
                    state: JobState::Ready,
                    reserver: None,
                    reserve_ct: r.reserve_ct,
                    timeout_ct: r.timeout_ct,
                    release_ct: r.release_ct,
                    bury_ct: r.bury_ct,
                    kick_ct: r.kick_ct,
                    // The reference reports the file holding the job's full
                    // record; that is the store's business and the field is
                    // masked in differential tests.
                    file: 0,
                },
            );
            if let Some(t) = e.tube_mut(tube) {
                t.job_ref_ct += 1;
            }
            match r.state {
                // `bury_job(s, j, 0)`: increments bury_ct once more.
                RecordState::Buried => e.insert_buried(tube, r.id),
                RecordState::Delayed if r.deadline_at > now => {
                    e.insert_delayed(tube, r.id, r.deadline_at);
                }
                RecordState::Delayed | RecordState::Ready => e.insert_ready(tube, r.id),
            }
        }
        // `recovery.next_id` is authoritative (it also covers deleted jobs
        // whose records survive); never go below 1 or reuse a live id.
        e.next_job_id = recovery.next_id.max(max_id + 1).max(1);
        e
    }

    /// Moves every pending journal entry into `buf`, in order.
    pub fn take_journal(&mut self, buf: &mut Vec<JournalEntry>) {
        buf.append(&mut self.journal);
    }

    pub fn set_binlog_stats(&mut self, stats: BinlogStats) {
        self.binlog = stats;
    }

    /// Monitoring view (docs/PLAN.md §5.3 decision 4): `server` is exactly
    /// what `stats` would report at `now` (before that command's own
    /// `cmd-stats` increment) and `tubes` is what `stats-tube` would report
    /// for every tube, in `list-tubes` order. Read-only: no counter moves
    /// and no journal entry is produced.
    pub fn snapshot(&self, now: Nanos) -> crate::Snapshot {
        self.snapshot_limited(now, usize::MAX)
    }

    /// Like [`Engine::snapshot`], but `tubes` holds only the first
    /// `max_tubes` tubes in `list-tubes` order, so the work done is
    /// bounded by `max_tubes` rather than by the number of tubes.
    /// `server` is complete (`current_tubes` is still the full count).
    pub fn snapshot_limited(&self, now: Nanos, max_tubes: usize) -> crate::Snapshot {
        crate::Snapshot {
            server: self.build_stats_server(now),
            tubes: self
                .tube_order
                .items
                .iter()
                .take(max_tubes)
                .filter_map(|&tid| self.tube(tid))
                .map(|t| Self::stats_tube_of(t, now))
                .collect(),
        }
    }
}

// ---------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------
impl Engine {
    /// The persistent record of `j` as it is now (`j->r` in the
    /// reference), for a journal entry.
    fn job_record(j: &JobRec) -> JobRecord {
        let (state, deadline_at) = match j.state {
            JobState::Delayed => (RecordState::Delayed, j.deadline_at),
            JobState::Buried => (RecordState::Buried, 0),
            // Reserved is never journaled; map it the way readrec does.
            JobState::Ready | JobState::Reserved => (RecordState::Ready, 0),
        };
        JobRecord {
            id: j.id,
            pri: j.pri,
            delay: j.delay,
            ttr: j.ttr,
            created_at: j.created_at,
            deadline_at,
            state,
            reserve_ct: j.reserve_ct,
            timeout_ct: j.timeout_ct,
            release_ct: j.release_ct,
            bury_ct: j.bury_ct,
            kick_ct: j.kick_ct,
        }
    }

    /// Journals a new job (the reference's full record, `filewrjobfull`).
    /// Called from `cmd_put` right after the job is queued and before
    /// `process_queue`, where `enqueue_job` calls `walwrite`.
    fn journal_put(&mut self, id: JobId) {
        if !self.cfg.journal {
            return;
        }
        let Some(j) = self.jobs.get(&id) else {
            return;
        };
        let entry = JournalEntry::Put {
            record: Self::job_record(j),
            tube: self.tube_name(j.tube),
            body: j.body.clone(),
        };
        self.journal.push(entry);
    }

    /// Journals a later transition of job `id` (a short record): release
    /// with delay, bury, kick and kick-job.
    fn journal_update(&mut self, id: JobId) {
        if !self.cfg.journal {
            return;
        }
        if let Some(j) = self.jobs.get(&id) {
            let record = Self::job_record(j);
            self.journal.push(JournalEntry::Update(record));
        }
    }

    /// Journals a deletion (`j->r.state = Invalid; walwrite(...)`).
    fn journal_delete(&mut self, id: JobId) {
        if self.cfg.journal {
            self.journal.push(JournalEntry::Delete(id));
        }
    }

    fn tube(&self, id: TubeId) -> Option<&TubeState> {
        self.tubes.get(id).and_then(Option::as_ref)
    }

    fn tube_mut(&mut self, id: TubeId) -> Option<&mut TubeState> {
        self.tubes.get_mut(id).and_then(Option::as_mut)
    }

    fn tube_name(&self, id: TubeId) -> TubeName {
        self.tube(id)
            .map(|t| t.name.clone())
            .unwrap_or_else(TubeName::default_tube)
    }

    fn find_or_make_tube(&mut self, name: &TubeName) -> TubeId {
        if let Some(&id) = self.tube_ids.get(name) {
            return id;
        }
        let state = TubeState::new(name.clone(), self.tube_order.len());
        let id = match self.free_tube_ids.pop() {
            Some(id) => {
                self.tubes[id] = Some(state);
                id
            }
            None => {
                self.tubes.push(Some(state));
                self.tubes.len() - 1
            }
        };
        self.tube_ids.insert(name.clone(), id);
        self.tube_order.append(id);
        id
    }

    /// Destroys a tube if it has no more uses, watchers or jobs. "default"
    /// is immortal (mirrors the permanent reference held by the reference
    /// implementation's static `default_tube` pointer).
    fn gc_tube_if_orphan(&mut self, id: TubeId) {
        if id == DEFAULT_TUBE {
            return;
        }
        if !self.tube(id).is_some_and(|t| t.refs() == 0) {
            return;
        }
        let Some(t) = self.tubes.get_mut(id).and_then(Option::take) else {
            return;
        };
        self.tube_ids.remove(&t.name);
        if t.pause > 0 {
            self.pauses.remove(&(t.unpause_at, id));
        }
        if let Some(d) = t.delay_head {
            self.delay_heads.remove(&(d, id));
        }
        if t.dispatchable {
            self.dispatchable.remove(&id);
        }
        // `ms_remove`: swap-with-last; the moved tube takes this position.
        self.tube_order.remove_at(t.pos);
        if let Some(&moved) = self.tube_order.items.get(t.pos)
            && let Some(m) = self.tube_mut(moved)
        {
            m.pos = t.pos;
        }
        self.free_tube_ids.push(id);
    }

    /// Re-derives whether `tube` belongs in `dispatchable`.
    fn refresh_dispatchable(&mut self, tube: TubeId) {
        let Some(t) = self.tubes.get_mut(tube).and_then(Option::as_mut) else {
            return;
        };
        let want = !t.waiting_conns.is_empty() && !t.ready.is_empty();
        if want != t.dispatchable {
            t.dispatchable = want;
            if want {
                self.dispatchable.insert(tube);
            } else {
                self.dispatchable.remove(&tube);
            }
        }
    }

    /// Re-derives `tube`'s entry in `delay_heads`.
    fn refresh_delay_head(&mut self, tube: TubeId) {
        let Some(t) = self.tubes.get_mut(tube).and_then(Option::as_mut) else {
            return;
        };
        let head = t.delayed.first().map(|&(d, _)| d);
        if head != t.delay_head {
            if let Some(old) = t.delay_head {
                self.delay_heads.remove(&(old, tube));
            }
            if let Some(new) = head {
                self.delay_heads.insert((new, tube));
            }
            t.delay_head = head;
        }
    }

    /// Re-derives `cid`'s entry in `conn_ticks`. Must run after every change
    /// to the connection's reserved jobs, their deadlines, or its waiting
    /// state.
    fn refresh_conn_tick(&mut self, cid: ConnId) {
        let Some(c) = self.conns.get_mut(&cid) else {
            return;
        };
        let new = conn_tickat(c);
        if new != c.tick_key {
            if let Some(old) = c.tick_key {
                self.conn_ticks.remove(&(old, cid));
            }
            if let Some(n) = new {
                self.conn_ticks.insert((n, cid));
            }
            c.tick_key = new;
        }
    }

    /// Clears every pause that has expired by `now`, as the reference's
    /// `next_awaited_job` does for each tube it visits.
    fn clear_expired_pauses(&mut self, now: Nanos) {
        while let Some(&(at, tube)) = self.pauses.first() {
            if at > now {
                break;
            }
            self.pauses.pop_first();
            if let Some(t) = self.tube_mut(tube) {
                t.pause = 0;
            }
        }
    }

    fn insert_ready(&mut self, tube: TubeId, id: JobId) {
        let pri = match self.jobs.get_mut(&id) {
            Some(j) => {
                j.state = JobState::Ready;
                j.reserver = None;
                j.pri
            }
            None => return,
        };
        if let Some(t) = self.tube_mut(tube) {
            t.ready.insert((pri, id));
            if pri < URGENT_THRESHOLD {
                t.stat.urgent_ct += 1;
            }
        }
        self.ready_ct += 1;
        if pri < URGENT_THRESHOLD {
            self.urgent_ct += 1;
        }
        self.refresh_dispatchable(tube);
    }

    fn remove_ready(&mut self, tube: TubeId, id: JobId) {
        let pri = self.jobs.get(&id).map(|j| j.pri).unwrap_or(0);
        if let Some(t) = self.tube_mut(tube) {
            t.ready.remove(&(pri, id));
            if pri < URGENT_THRESHOLD {
                t.stat.urgent_ct = t.stat.urgent_ct.saturating_sub(1);
            }
        }
        self.ready_ct = self.ready_ct.saturating_sub(1);
        if pri < URGENT_THRESHOLD {
            self.urgent_ct = self.urgent_ct.saturating_sub(1);
        }
        self.refresh_dispatchable(tube);
    }

    fn insert_delayed(&mut self, tube: TubeId, id: JobId, deadline: Nanos) {
        if let Some(j) = self.jobs.get_mut(&id) {
            j.state = JobState::Delayed;
            j.reserver = None;
            j.deadline_at = deadline;
        }
        if let Some(t) = self.tube_mut(tube)
            && t.delayed.insert((deadline, id))
        {
            self.delayed_ct += 1;
        }
        self.refresh_delay_head(tube);
    }

    fn remove_delayed(&mut self, tube: TubeId, id: JobId) {
        let deadline = self.jobs.get(&id).map(|j| j.deadline_at).unwrap_or(0);
        if let Some(t) = self.tube_mut(tube)
            && t.delayed.remove(&(deadline, id))
        {
            self.delayed_ct = self.delayed_ct.saturating_sub(1);
        }
        self.refresh_delay_head(tube);
    }

    fn insert_buried(&mut self, tube: TubeId, id: JobId) {
        if let Some(j) = self.jobs.get_mut(&id) {
            j.state = JobState::Buried;
            j.reserver = None;
            j.bury_ct += 1;
        }
        if let Some(t) = self.tube_mut(tube) {
            t.buried.push_back(id);
            t.stat.buried_ct += 1;
        }
        self.buried_ct += 1;
    }

    /// Removes a specific job from the buried FIFO (used by delete and
    /// kick-job, which can target any buried job, not just the front).
    fn remove_buried(&mut self, tube: TubeId, id: JobId) -> bool {
        let removed = match self.tube_mut(tube) {
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

    fn pop_buried_front(&mut self, tube: TubeId) -> Option<JobId> {
        let t = self.tube_mut(tube)?;
        let id = t.buried.pop_front()?;
        t.stat.buried_ct = t.stat.buried_ct.saturating_sub(1);
        self.buried_ct = self.buried_ct.saturating_sub(1);
        Some(id)
    }

    /// `conn_reserve_job`: assigns `job_id` to `cid`, sets its TTR
    /// deadline, and updates every reserved-job counter.
    fn do_reserve(&mut self, cid: ConnId, job_id: JobId, now: Nanos) {
        let Some(j) = self.jobs.get_mut(&job_id) else {
            return;
        };
        let tube = j.tube;
        let deadline = now + (j.ttr as Nanos) * NANOS_PER_SEC;
        j.state = JobState::Reserved;
        j.reserver = Some(cid);
        j.deadline_at = deadline;
        j.reserve_ct += 1;
        if let Some(c) = self.conns.get_mut(&cid) {
            c.reserved_fifo.push(job_id);
            c.reserved_by_deadline.insert((deadline, job_id));
        }
        self.reserved_ct += 1;
        if let Some(t) = self.tube_mut(tube) {
            t.stat.reserved_ct += 1;
        }
        self.refresh_conn_tick(cid);
    }

    /// Removes `job_id` from `cid`'s reservation bookkeeping and decrements
    /// the reserved-job counters. Does not change the job's state; the
    /// caller decides what happens to the job next.
    fn do_unreserve(&mut self, cid: ConnId, job_id: JobId) {
        let (deadline, tube) = match self.jobs.get_mut(&job_id) {
            Some(j) => {
                j.reserver = None;
                (j.deadline_at, Some(j.tube))
            }
            None => (0, None),
        };
        if let Some(c) = self.conns.get_mut(&cid) {
            if let Some(i) = c.reserved_fifo.iter().position(|&x| x == job_id) {
                c.reserved_fifo.remove(i);
            }
            c.reserved_by_deadline.remove(&(deadline, job_id));
        }
        self.reserved_ct = self.reserved_ct.saturating_sub(1);
        if let Some(t) = tube.and_then(|t| self.tube_mut(t)) {
            t.stat.reserved_ct = t.stat.reserved_ct.saturating_sub(1);
        }
        self.refresh_conn_tick(cid);
    }

    fn enqueue_waiting_conn(&mut self, cid: ConnId, wait_deadline: Option<Nanos>) {
        let watched = match self.conns.get_mut(&cid) {
            Some(c) => {
                c.waiting = true;
                c.wait_deadline = wait_deadline;
                std::mem::take(&mut c.watch)
            }
            None => return,
        };
        self.waiting_ct += 1;
        for &t in &watched.items {
            if let Some(ts) = self.tube_mut(t) {
                ts.stat.waiting_ct += 1;
                ts.waiting_conns.append(cid);
            }
            self.refresh_dispatchable(t);
        }
        if let Some(c) = self.conns.get_mut(&cid) {
            c.watch = watched;
        }
        self.refresh_conn_tick(cid);
    }

    fn do_remove_waiting_conn(&mut self, cid: ConnId) {
        let watched = match self.conns.get_mut(&cid) {
            Some(c) if c.waiting => {
                c.waiting = false;
                c.wait_deadline = None;
                std::mem::take(&mut c.watch)
            }
            _ => return,
        };
        self.waiting_ct = self.waiting_ct.saturating_sub(1);
        for &t in &watched.items {
            if let Some(ts) = self.tube_mut(t) {
                ts.stat.waiting_ct = ts.stat.waiting_ct.saturating_sub(1);
                ts.waiting_conns.remove(&cid);
            }
            self.refresh_dispatchable(t);
        }
        if let Some(c) = self.conns.get_mut(&cid) {
            c.watch = watched;
        }
        self.refresh_conn_tick(cid);
    }

    /// `process_queue`: repeatedly assigns the globally best (pri, id)
    /// ready job to a waiting connection, across every watched/unpaused
    /// tube, until no more assignments are possible. Mirrors
    /// `next_awaited_job`'s side effect of auto-clearing expired pauses.
    ///
    /// Only `dispatchable` tubes (waiters and ready jobs) can match. The
    /// order they are visited in does not matter: `(pri, id)` is unique, so
    /// the minimum is the same whichever tube is seen first.
    fn process_queue(&mut self, now: Nanos, out: &mut Outbox) {
        self.clear_expired_pauses(now);
        loop {
            let mut best: Option<(u32, JobId, TubeId)> = None;
            for &tid in &self.dispatchable {
                let Some(t) = self.tube(tid) else {
                    continue;
                };
                // Every pause left after `clear_expired_pauses` is still
                // in effect.
                if t.pause > 0 {
                    continue;
                }
                if let Some(&(pri, id)) = t.ready.first()
                    && best.is_none_or(|(bp, bid, _)| (pri, id) < (bp, bid))
                {
                    best = Some((pri, id, tid));
                }
            }
            let Some((_, id, tube)) = best else { break };
            self.remove_ready(tube, id);
            let taken = self.tube_mut(tube).and_then(|t| t.waiting_conns.take());
            self.refresh_dispatchable(tube);
            let Some(cid) = taken else {
                // Defensive: mirrors the reference's `if (c == NULL)` guard;
                // should not happen since the tube was dispatchable.
                continue;
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
    /// across all tubes. Ties are broken by tube array order (the tube
    /// earliest in `tube_order` wins), matching the reference's strict `<`
    /// comparison over `tubes.items` in order.
    fn soonest_delayed_job(&self) -> Option<(Nanos, TubeId, JobId)> {
        let &(deadline, _) = self.delay_heads.first()?;
        let mut best: Option<(usize, TubeId)> = None;
        for &(_, tid) in self
            .delay_heads
            .range((deadline, 0)..=(deadline, TubeId::MAX))
        {
            let Some(t) = self.tube(tid) else {
                continue;
            };
            if best.is_none_or(|(pos, _)| t.pos < pos) {
                best = Some((t.pos, tid));
            }
        }
        let (_, tube) = best?;
        let &(d, id) = self.tube(tube)?.delayed.first()?;
        Some((d, tube, id))
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
        c.watch
            .items
            .iter()
            .any(|&t| self.tube(t).is_some_and(|ts| !ts.ready.is_empty()))
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
            let tube = self.jobs.get(&job_id).map(|j| j.tube);
            self.do_unreserve(cid, job_id);
            self.timeout_ct += 1;
            if let Some(j) = self.jobs.get_mut(&job_id) {
                j.timeout_ct += 1;
            }
            if let Some(tube) = tube {
                self.insert_ready(tube, job_id);
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

    fn kick_to_ready(&mut self, tube: TubeId, id: JobId, now: Nanos, out: &mut Outbox) {
        if let Some(j) = self.jobs.get_mut(&id) {
            j.kick_ct += 1;
        }
        self.insert_ready(tube, id);
        // kick_buried_job / kick_delayed_job: `enqueue_job(s, j, 0, 1)`
        // writes the (now ready) record before `process_queue`.
        self.journal_update(id);
        self.process_queue(now, out);
    }

    fn use_tube_of(&self, cid: ConnId) -> TubeId {
        self.conns.get(&cid).map_or(DEFAULT_TUBE, |c| c.use_tube)
    }

    /// Body of the job `pick` selects from `cid`'s used tube, as a FOUND
    /// reply (NOT_FOUND if there is none).
    fn peek_used_tube(&self, cid: ConnId, pick: impl Fn(&TubeState) -> Option<JobId>) -> Response {
        let top = self.tube(self.use_tube_of(cid)).and_then(pick);
        match top {
            Some(id) => {
                let body = self
                    .jobs
                    .get(&id)
                    .map(|j| j.body.clone())
                    .unwrap_or_default();
                Response::Found { id, body }
            }
            None => Response::NotFound,
        }
    }
}

// ---------------------------------------------------------------------
// State validation (P3 snapshots)
// ---------------------------------------------------------------------
impl Engine {
    /// Checks every structural invariant the engine relies on, recomputing
    /// each index and counter from scratch and comparing it with the stored
    /// value. Used by `import_state` on untrusted snapshots, and by the test
    /// invariant checks. Never panics: only `get`-style lookups, and sums
    /// are bounded by collection sizes.
    ///
    /// Accepted as-is (history that cannot be recomputed): `cfg`, `start`,
    /// `draining`, `binlog`, `cmd_*`, `total_jobs_ct`, `timeout_ct`,
    /// per-tube `total_jobs_ct` / `total_delete_ct` / `pause_ct`, the
    /// `unpause_at` of an unpaused tube, the `deadline_at` of a ready or
    /// buried job, `created_at`, the job counters, and every `Ms` cursor.
    pub(crate) fn validate(&self) -> Result<(), String> {
        macro_rules! ensure {
            ($cond:expr, $($arg:tt)+) => {
                if !$cond {
                    return Err(format!($($arg)+));
                }
            };
        }
        let n_tubes = self.tubes.len();

        // --- Tube slab, names, list order, free list -------------------
        let default = self
            .tube(DEFAULT_TUBE)
            .ok_or_else(|| "tube 0 (default) is missing".to_string())?;
        ensure!(
            default.name == TubeName::default_tube(),
            "tube 0 is {:?}, not default",
            default.name
        );
        ensure!(
            default.pos == 0,
            "default tube is not first in the tube list"
        );
        let mut live = 0usize;
        for (tid, t) in self.tubes.iter().enumerate() {
            let Some(t) = t else { continue };
            live += 1;
            ensure!(
                self.tube_ids.get(&t.name) == Some(&tid),
                "tube_ids does not map {:?} to tube {tid}",
                t.name
            );
            ensure!(
                self.tube_order.items.get(t.pos) == Some(&tid),
                "tube {tid} is not at its position {} in the tube list",
                t.pos
            );
        }
        // With the per-tube checks above, equal sizes make `tube_ids` and
        // `tube_order` exact images of the live slots.
        ensure!(
            self.tube_ids.len() == live,
            "tube_ids has {} entries for {live} tubes",
            self.tube_ids.len()
        );
        ensure!(
            self.tube_order.len() == live,
            "tube list has {} entries for {live} tubes",
            self.tube_order.len()
        );
        let free_slots = n_tubes - live;
        ensure!(
            self.free_tube_ids.len() == free_slots,
            "free list has {} entries for {free_slots} free slots",
            self.free_tube_ids.len()
        );
        let mut seen_free: HashSet<TubeId> = HashSet::with_capacity(free_slots);
        for &id in &self.free_tube_ids {
            ensure!(
                matches!(self.tubes.get(id), Some(None)),
                "free list names tube {id}, which is not a free slot"
            );
            ensure!(seen_free.insert(id), "free list repeats tube {id}");
        }

        ensure!(self.next_job_id >= 1, "next job id is 0");

        // --- Connections -------------------------------------------------
        let mut using = vec![0u64; n_tubes];
        let mut watching = vec![0u64; n_tubes];
        let mut waiters = vec![0u64; n_tubes];
        // (conn, tube) for every waiting connection and tube it watches.
        let mut wait_pairs: HashSet<(ConnId, TubeId)> = HashSet::new();
        let mut producers = 0u64;
        let mut workers = 0u64;
        let mut waiting = 0u64;
        let mut reserved_entries = 0u64;
        let mut pending_ids: HashSet<JobId> = HashSet::new();
        let mut conn_ticks: BTreeSet<(Nanos, ConnId)> = BTreeSet::new();
        for (&cid, c) in &self.conns {
            ensure!(
                self.tube(c.use_tube).is_some(),
                "conn {cid} uses missing tube {}",
                c.use_tube
            );
            if let Some(u) = using.get_mut(c.use_tube) {
                *u += 1;
            }
            ensure!(!c.watch.is_empty(), "conn {cid} watches no tube");
            let mut seen: HashSet<TubeId> = HashSet::with_capacity(c.watch.len());
            for &t in &c.watch.items {
                ensure!(
                    self.tube(t).is_some(),
                    "conn {cid} watches missing tube {t}"
                );
                ensure!(seen.insert(t), "conn {cid} watches tube {t} twice");
                if let Some(w) = watching.get_mut(t) {
                    *w += 1;
                }
                if c.waiting {
                    if let Some(w) = waiters.get_mut(t) {
                        *w += 1;
                    }
                    wait_pairs.insert((cid, t));
                }
            }
            if c.waiting {
                waiting += 1;
            } else {
                ensure!(
                    c.wait_deadline.is_none(),
                    "conn {cid} has a wait deadline but is not waiting"
                );
            }
            producers += u64::from(c.is_producer);
            workers += u64::from(c.is_worker);
            if let Some(PendingPut { id: Some(id), .. }) = c.pending_put {
                ensure!(
                    id < self.next_job_id,
                    "conn {cid} has pending job id {id} >= next id {}",
                    self.next_job_id
                );
                ensure!(
                    !self.jobs.contains_key(&id),
                    "conn {cid} has pending job id {id}, which is a live job"
                );
                ensure!(pending_ids.insert(id), "pending job id {id} is shared");
            }

            // Reservations: `reserved_fifo` and `reserved_by_deadline` hold
            // the same jobs, each reserved by this connection.
            ensure!(
                c.reserved_fifo.len() == c.reserved_by_deadline.len(),
                "conn {cid}: reservation lists differ in length"
            );
            let mut seen: HashSet<JobId> = HashSet::with_capacity(c.reserved_fifo.len());
            for &id in &c.reserved_fifo {
                ensure!(seen.insert(id), "conn {cid} reserves job {id} twice");
                let j = self
                    .jobs
                    .get(&id)
                    .ok_or_else(|| format!("conn {cid} reserves missing job {id}"))?;
                ensure!(
                    j.state == JobState::Reserved && j.reserver == Some(cid),
                    "conn {cid} lists job {id}, which it has not reserved"
                );
            }
            for &(d, id) in &c.reserved_by_deadline {
                ensure!(
                    seen.contains(&id),
                    "conn {cid}: job {id} is in the deadline index only"
                );
                ensure!(
                    self.jobs.get(&id).is_some_and(|j| j.deadline_at == d),
                    "conn {cid}: stale deadline for job {id}"
                );
            }
            reserved_entries += c.reserved_fifo.len() as u64;

            let tick = conn_tickat(c);
            ensure!(c.tick_key == tick, "tick_key of conn {cid} is stale");
            if let Some(t) = tick {
                conn_ticks.insert((t, cid));
            }
        }
        ensure!(self.conn_ticks == conn_ticks, "conn_ticks index is stale");
        ensure!(
            self.cur_conns as usize == self.conns.len(),
            "cur_conns is {} for {} conns",
            self.cur_conns,
            self.conns.len()
        );
        ensure!(
            self.tot_conns >= self.cur_conns,
            "tot_conns below cur_conns"
        );
        ensure!(
            u64::from(self.cur_producers) == producers,
            "cur_producers is {}, recomputed {producers}",
            self.cur_producers
        );
        ensure!(
            u64::from(self.cur_workers) == workers,
            "cur_workers is {}, recomputed {workers}",
            self.cur_workers
        );
        ensure!(
            self.waiting_ct == waiting,
            "waiting_ct is {}, recomputed {waiting}",
            self.waiting_ct
        );

        // --- Jobs --------------------------------------------------------
        let mut job_refs = vec![0u64; n_tubes];
        let mut reserved_in = vec![0u64; n_tubes];
        let mut n_ready = 0u64;
        let mut n_delayed = 0u64;
        let mut n_buried = 0u64;
        let mut n_reserved = 0u64;
        for (&id, j) in &self.jobs {
            ensure!(j.id == id, "job stored under id {id} has id {}", j.id);
            ensure!(
                id < self.next_job_id,
                "job {id} is not below next id {}",
                self.next_job_id
            );
            ensure!(j.ttr >= 1, "job {id} has ttr 0");
            let t = self
                .tube(j.tube)
                .ok_or_else(|| format!("job {id} is in missing tube {}", j.tube))?;
            if let Some(r) = job_refs.get_mut(j.tube) {
                *r += 1;
            }
            if j.state == JobState::Reserved {
                let cid = j
                    .reserver
                    .ok_or_else(|| format!("reserved job {id} has no reserver"))?;
                ensure!(
                    self.conns.contains_key(&cid),
                    "job {id} is reserved by missing conn {cid}"
                );
            } else {
                ensure!(j.reserver.is_none(), "unreserved job {id} has a reserver");
            }
            match j.state {
                JobState::Ready => {
                    n_ready += 1;
                    ensure!(
                        t.ready.contains(&(j.pri, id)),
                        "ready job {id} is not in its tube's ready set"
                    );
                }
                JobState::Delayed => {
                    n_delayed += 1;
                    ensure!(
                        t.delayed.contains(&(j.deadline_at, id)),
                        "delayed job {id} is not in its tube's delayed set"
                    );
                }
                JobState::Buried => n_buried += 1,
                JobState::Reserved => {
                    n_reserved += 1;
                    if let Some(r) = reserved_in.get_mut(j.tube) {
                        *r += 1;
                    }
                }
            }
        }
        // Each reservation entry names a distinct job reserved by that conn;
        // equal counts make them the exact set of reserved jobs.
        ensure!(
            reserved_entries == n_reserved,
            "{reserved_entries} reservation entries for {n_reserved} reserved jobs"
        );
        ensure!(
            self.reserved_ct == n_reserved,
            "reserved_ct is {}, recomputed {n_reserved}",
            self.reserved_ct
        );

        // --- Per-tube sets, counters and indexes -------------------------
        let mut ready_entries = 0u64;
        let mut urgent = 0u64;
        let mut delayed_entries = 0u64;
        let mut buried_entries = 0u64;
        let mut seen_buried: HashSet<JobId> = HashSet::new();
        let mut delay_heads: BTreeSet<(Nanos, TubeId)> = BTreeSet::new();
        let mut pauses: BTreeSet<(Nanos, TubeId)> = BTreeSet::new();
        let mut dispatchable: BTreeSet<TubeId> = BTreeSet::new();
        for (tid, t) in self.tubes.iter().enumerate() {
            let Some(t) = t else { continue };
            let mut tube_urgent = 0u64;
            for &(pri, id) in &t.ready {
                ensure!(
                    self.jobs
                        .get(&id)
                        .is_some_and(|j| j.state == JobState::Ready
                            && j.tube == tid
                            && j.pri == pri),
                    "ready set of tube {tid} holds a stale entry for job {id}"
                );
                if pri < URGENT_THRESHOLD {
                    tube_urgent += 1;
                }
            }
            for &(d, id) in &t.delayed {
                ensure!(
                    self.jobs
                        .get(&id)
                        .is_some_and(|j| j.state == JobState::Delayed
                            && j.tube == tid
                            && j.deadline_at == d),
                    "delayed set of tube {tid} holds a stale entry for job {id}"
                );
            }
            for &id in &t.buried {
                ensure!(
                    self.jobs
                        .get(&id)
                        .is_some_and(|j| j.state == JobState::Buried && j.tube == tid),
                    "buried list of tube {tid} holds a stale entry for job {id}"
                );
                ensure!(seen_buried.insert(id), "job {id} is buried twice");
            }
            let mut seen: HashSet<ConnId> = HashSet::with_capacity(t.waiting_conns.len());
            for &cid in &t.waiting_conns.items {
                ensure!(
                    wait_pairs.contains(&(cid, tid)),
                    "tube {tid} lists conn {cid} as waiting"
                );
                ensure!(seen.insert(cid), "tube {tid} lists conn {cid} twice");
            }
            let get = |v: &[u64]| v.get(tid).copied().unwrap_or(0);
            ensure!(
                t.waiting_conns.len() as u64 == get(&waiters),
                "tube {tid} misses waiting conns"
            );
            ensure!(
                t.stat.urgent_ct == tube_urgent,
                "urgent count of tube {tid} is {}, recomputed {tube_urgent}",
                t.stat.urgent_ct
            );
            ensure!(
                t.stat.buried_ct == t.buried.len() as u64,
                "buried count of tube {tid} is stale"
            );
            ensure!(
                t.stat.reserved_ct == get(&reserved_in),
                "reserved count of tube {tid} is stale"
            );
            ensure!(
                t.stat.waiting_ct == get(&waiters),
                "waiting count of tube {tid} is stale"
            );
            ensure!(
                u64::from(t.using_ct) == get(&using),
                "using count of tube {tid} is stale"
            );
            ensure!(
                u64::from(t.watching_ct) == get(&watching),
                "watching count of tube {tid} is stale"
            );
            ensure!(
                t.job_ref_ct == get(&job_refs),
                "job count of tube {tid} is stale"
            );
            // Unreferenced tubes are destroyed at once (except default).
            ensure!(
                tid == DEFAULT_TUBE || get(&using) + get(&watching) + get(&job_refs) > 0,
                "tube {tid} is unreferenced"
            );

            let head = t.delayed.first().map(|&(d, _)| d);
            ensure!(t.delay_head == head, "delay_head of tube {tid} is stale");
            if let Some(d) = head {
                delay_heads.insert((d, tid));
            }
            if t.pause > 0 {
                pauses.insert((t.unpause_at, tid));
            }
            let disp = !t.waiting_conns.is_empty() && !t.ready.is_empty();
            ensure!(
                t.dispatchable == disp,
                "dispatchable flag of tube {tid} is stale"
            );
            if disp {
                dispatchable.insert(tid);
                // `process_queue` runs after every change that adds a
                // ready job or a waiter, so only a pause can hold them apart.
                ensure!(
                    t.pause > 0,
                    "tube {tid} has ready jobs and waiting conns but is not paused"
                );
            }

            ready_entries += t.ready.len() as u64;
            urgent += tube_urgent;
            delayed_entries += t.delayed.len() as u64;
            buried_entries += t.buried.len() as u64;
        }
        // Every entry names a distinct job in that state; equal counts make
        // the sets partition the jobs.
        ensure!(
            ready_entries == n_ready,
            "{ready_entries} ready entries for {n_ready} ready jobs"
        );
        ensure!(
            delayed_entries == n_delayed,
            "{delayed_entries} delayed entries for {n_delayed} delayed jobs"
        );
        ensure!(
            buried_entries == n_buried,
            "{buried_entries} buried entries for {n_buried} buried jobs"
        );
        ensure!(
            self.ready_ct == n_ready,
            "ready_ct is {}, recomputed {n_ready}",
            self.ready_ct
        );
        ensure!(
            self.urgent_ct == urgent,
            "urgent_ct is {}, recomputed {urgent}",
            self.urgent_ct
        );
        ensure!(
            self.delayed_ct == n_delayed,
            "delayed_ct is {}, recomputed {n_delayed}",
            self.delayed_ct
        );
        ensure!(
            self.buried_ct == n_buried,
            "buried_ct is {}, recomputed {n_buried}",
            self.buried_ct
        );
        ensure!(
            self.delay_heads == delay_heads,
            "delay_heads index is stale"
        );
        ensure!(self.pauses == pauses, "pauses index is stale");
        ensure!(
            self.dispatchable == dispatchable,
            "dispatchable set is stale"
        );
        Ok(())
    }
}

/// `conntickat`: the absolute time at which this connection next needs
/// attention, if any.
fn conn_tickat(c: &ConnState) -> Option<Nanos> {
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

        // The used tube always exists: the connection holds a reference.
        let tube = self.use_tube_of(cid);

        let job = JobRec {
            id,
            tube,
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
            // Approximation: the index of the binlog file the server last
            // reported, not necessarily the one this job's first record
            // lands in (the store may start a new file for it). The
            // reference reports `j->file->seq`, which also changes when
            // compaction moves the job; the field is masked in differential
            // tests.
            file: if self.cfg.journal {
                self.binlog.current_index
            } else {
                0
            },
        };
        self.jobs.insert(id, job);
        if let Some(t) = self.tube_mut(tube) {
            t.job_ref_ct += 1;
        }

        if delay > 0 {
            let deadline = now + (delay as Nanos) * NANOS_PER_SEC;
            self.insert_delayed(tube, id, deadline);
        } else {
            self.insert_ready(tube, id);
        }
        // `enqueue_job(c->srv, j, j->r.delay, 1)` writes the record before
        // `process_queue` can hand the job to a waiting connection.
        self.journal_put(id);
        self.process_queue(now, out);

        self.total_jobs_ct += 1;
        if let Some(t) = self.tube_mut(tube) {
            t.stat.total_jobs_ct += 1;
        }
        out.push((cid, Response::Inserted(id)));
    }

    fn cmd_use(&mut self, cid: ConnId, tube: TubeName, out: &mut Outbox) {
        self.cmd_use += 1;
        let new = self.find_or_make_tube(&tube);
        let old = self.use_tube_of(cid);
        if old != new {
            if let Some(t) = self.tube_mut(old) {
                t.using_ct = t.using_ct.saturating_sub(1);
            }
            self.gc_tube_if_orphan(old);
            if let Some(t) = self.tube_mut(new) {
                t.using_ct += 1;
            }
            if let Some(c) = self.conns.get_mut(&cid) {
                c.use_tube = new;
            }
        }
        out.push((cid, Response::Using(tube)));
    }

    fn cmd_watch(&mut self, cid: ConnId, tube: TubeName, out: &mut Outbox) {
        self.cmd_watch += 1;
        let tid = self.find_or_make_tube(&tube);
        let already = self
            .conns
            .get(&cid)
            .map(|c| c.watch.contains(&tid))
            .unwrap_or(true);
        if !already {
            if let Some(c) = self.conns.get_mut(&cid) {
                c.watch.append(tid);
            }
            if let Some(t) = self.tube_mut(tid) {
                t.watching_ct += 1;
            }
        }
        let count = self.conns.get(&cid).map(|c| c.watch.len()).unwrap_or(0);
        out.push((cid, Response::Watching(count as u64)));
    }

    fn cmd_ignore(&mut self, cid: ConnId, tube: TubeName, out: &mut Outbox) {
        self.cmd_ignore += 1;
        let tid = self.tube_ids.get(&tube).copied();
        let watching_it = match (tid, self.conns.get(&cid)) {
            (Some(tid), Some(c)) => c.watch.contains(&tid),
            _ => false,
        };
        let watch_len = self.conns.get(&cid).map(|c| c.watch.len()).unwrap_or(0);
        if watching_it && watch_len < 2 {
            out.push((cid, Response::NotIgnored));
            return;
        }
        if let (true, Some(tid)) = (watching_it, tid) {
            if let Some(c) = self.conns.get_mut(&cid) {
                c.watch.remove(&tid);
            }
            if let Some(t) = self.tube_mut(tid) {
                t.watching_ct = t.watching_ct.saturating_sub(1);
            }
            self.gc_tube_if_orphan(tid);
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
        // A connection already inside its safety margin enters
        // `conn_ticks` at or before `now` here, so the server's tick right
        // after this call sends DEADLINE_SOON (docs/COMPAT.md engine item 5).
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
        let Some((state, tube)) = self.jobs.get(&id).map(|j| (j.state, j.tube)) else {
            out.push((cid, Response::NotFound));
            return;
        };
        if state == JobState::Reserved {
            out.push((cid, Response::NotFound));
            return;
        }
        if state == JobState::Ready {
            self.remove_ready(tube, id);
        } else if state == JobState::Buried {
            self.remove_buried(tube, id);
        } else {
            self.remove_delayed(tube, id);
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
        let Some((state, reserver, tube)) =
            self.jobs.get(&id).map(|j| (j.state, j.reserver, j.tube))
        else {
            out.push((cid, Response::NotFound));
            return;
        };
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
                self.remove_ready(tube, id);
                true
            }
            JobState::Buried => {
                self.remove_buried(tube, id);
                true
            }
            JobState::Delayed => {
                self.remove_delayed(tube, id);
                true
            }
        };
        if !ok {
            out.push((cid, Response::NotFound));
            return;
        }
        if let Some(t) = self.tube_mut(tube) {
            t.stat.total_delete_ct += 1;
            t.job_ref_ct = t.job_ref_ct.saturating_sub(1);
        }
        self.jobs.remove(&id);
        self.journal_delete(id);
        self.gc_tube_if_orphan(tube);
        out.push((cid, Response::Deleted));
    }

    /// The tube of `id` if `cid` holds its reservation.
    fn reserved_by(&self, cid: ConnId, id: JobId) -> Option<TubeId> {
        self.jobs
            .get(&id)
            .filter(|j| j.reserver == Some(cid) && j.state == JobState::Reserved)
            .map(|j| j.tube)
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
        let Some(tube) = self.reserved_by(cid, id) else {
            out.push((cid, Response::NotFound));
            return;
        };
        self.do_unreserve(cid, id);
        if let Some(j) = self.jobs.get_mut(&id) {
            j.pri = pri;
            j.delay = delay;
            j.release_ct += 1;
        }
        if delay > 0 {
            let deadline = now + (delay as Nanos) * NANOS_PER_SEC;
            self.insert_delayed(tube, id, deadline);
            // `enqueue_job(c->srv, j, delay, !!delay)`: only a release with
            // a delay is written.
            self.journal_update(id);
        } else {
            self.insert_ready(tube, id);
        }
        self.process_queue(now, out);
        out.push((cid, Response::Released));
    }

    fn cmd_bury(&mut self, cid: ConnId, id: JobId, pri: u32, out: &mut Outbox) {
        self.cmd_bury += 1;
        let Some(tube) = self.reserved_by(cid, id) else {
            out.push((cid, Response::NotFound));
            return;
        };
        self.do_unreserve(cid, id);
        if let Some(j) = self.jobs.get_mut(&id) {
            j.pri = pri;
        }
        self.insert_buried(tube, id);
        // `bury_job(c->srv, j, 1)`.
        self.journal_update(id);
        out.push((cid, Response::Buried));
    }

    fn cmd_touch(&mut self, now: Nanos, cid: ConnId, id: JobId, out: &mut Outbox) {
        self.cmd_touch += 1;
        if self.reserved_by(cid, id).is_none() {
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
        self.refresh_conn_tick(cid);
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
        let reply = self.peek_used_tube(cid, |t| t.ready.first().map(|&(_, id)| id));
        out.push((cid, reply));
    }

    fn cmd_peek_delayed(&mut self, cid: ConnId, out: &mut Outbox) {
        self.cmd_peek_delayed += 1;
        let reply = self.peek_used_tube(cid, |t| t.delayed.first().map(|&(_, id)| id));
        out.push((cid, reply));
    }

    fn cmd_peek_buried(&mut self, cid: ConnId, out: &mut Outbox) {
        self.cmd_peek_buried += 1;
        let reply = self.peek_used_tube(cid, |t| t.buried.front().copied());
        out.push((cid, reply));
    }

    fn cmd_kick(&mut self, now: Nanos, cid: ConnId, n: u32, out: &mut Outbox) {
        self.cmd_kick += 1;
        let tube = self.use_tube_of(cid);
        let has_buried = self.tube(tube).is_some_and(|t| !t.buried.is_empty());
        let mut count: u64 = 0;
        if has_buried {
            for _ in 0..n {
                let Some(id) = self.pop_buried_front(tube) else {
                    break;
                };
                self.kick_to_ready(tube, id, now, out);
                count += 1;
            }
        } else {
            for _ in 0..n {
                let next = self.tube(tube).and_then(|t| t.delayed.first().copied());
                let Some((_, id)) = next else { break };
                self.remove_delayed(tube, id);
                self.kick_to_ready(tube, id, now, out);
                count += 1;
            }
        }
        out.push((cid, Response::Kicked(count)));
    }

    fn cmd_kick_job(&mut self, now: Nanos, cid: ConnId, id: JobId, out: &mut Outbox) {
        let Some((state, tube)) = self.jobs.get(&id).map(|j| (j.state, j.tube)) else {
            out.push((cid, Response::NotFound));
            return;
        };
        match state {
            JobState::Buried => {
                self.remove_buried(tube, id);
                self.kick_to_ready(tube, id, now, out);
                out.push((cid, Response::KickedJob));
            }
            JobState::Delayed => {
                self.remove_delayed(tube, id);
                self.kick_to_ready(tube, id, now, out);
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
        let tube = self.tube_name(self.use_tube_of(cid));
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
        let Some(&tid) = self.tube_ids.get(&tube) else {
            out.push((cid, Response::NotFound));
            return;
        };
        // prot.c: `if (delay == 0) delay = 1;` runs on the delay already
        // converted to nanoseconds, so "pause 0" pauses for 1 ns (which
        // `stats-tube` reports as `pause: 0`), not for 1 second.
        let delay_nanos = if delay == 0 {
            1
        } else {
            (delay as Nanos) * NANOS_PER_SEC
        };
        let Some(t) = self.tubes.get_mut(tid).and_then(Option::as_mut) else {
            return;
        };
        if t.pause > 0 {
            self.pauses.remove(&(t.unpause_at, tid));
        }
        t.pause = delay_nanos;
        t.unpause_at = now + delay_nanos;
        t.stat.pause_ct += 1;
        self.pauses.insert((t.unpause_at, tid));
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
        self.tube_order
            .items
            .iter()
            .map(|&t| self.tube_name(t))
            .collect()
    }

    pub(crate) fn watched_tube_names(&self, cid: ConnId) -> Vec<TubeName> {
        self.conns
            .get(&cid)
            .map(|c| c.watch.items.iter().map(|&t| self.tube_name(t)).collect())
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
            tube: self.tube_name(j.tube),
            state: j.state_name(),
            pri: j.pri,
            age,
            delay: j.delay as u64,
            ttr: j.ttr as u64,
            time_left,
            file: j.file,
            reserves: j.reserve_ct as u64,
            timeouts: j.timeout_ct as u64,
            releases: j.release_ct as u64,
            buries: j.bury_ct as u64,
            kicks: j.kick_ct as u64,
        })
    }

    pub(crate) fn build_stats_tube(&self, name: &TubeName, now: Nanos) -> Option<StatsTube> {
        let t = self.tube(*self.tube_ids.get(name)?)?;
        Some(Self::stats_tube_of(t, now))
    }

    /// `stats-tube` data for one live tube (shared by `stats-tube` and
    /// `snapshot`).
    fn stats_tube_of(t: &TubeState, now: Nanos) -> StatsTube {
        let pause_time_left = if t.pause > 0 {
            t.unpause_at.saturating_sub(now) / NANOS_PER_SEC
        } else {
            0
        };
        StatsTube {
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
        }
    }

    pub(crate) fn build_stats_server(&self, now: Nanos) -> StatsServer {
        let snap = self.sys.snapshot();
        StatsServer {
            current_jobs_urgent: self.urgent_ct,
            current_jobs_ready: self.ready_ct,
            current_jobs_reserved: self.reserved_ct,
            current_jobs_delayed: self.delayed_ct,
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
            binlog_oldest_index: self.binlog.oldest_index,
            binlog_current_index: self.binlog.current_index,
            binlog_records_migrated: self.binlog.records_migrated,
            binlog_records_written: self.binlog.records_written,
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
    fn t_tube_by_name(&self, name: &TubeName) -> Option<&TubeState> {
        self.tube(*self.tube_ids.get(name)?)
    }

    pub(crate) fn t_journal_capacity(&self) -> usize {
        self.journal.capacity()
    }

    pub(crate) fn t_next_job_id(&self) -> JobId {
        self.next_job_id
    }

    /// A copy of job `id`'s internal record and its tube's name.
    pub(crate) fn t_job_raw(&self, id: JobId) -> Option<(JobRec, TubeName)> {
        self.jobs
            .get(&id)
            .map(|j| (j.clone(), self.tube_name(j.tube)))
    }

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
        self.t_tube_by_name(name).map(|t| t.ready.len())
    }

    pub(crate) fn t_tube_ready_ids(&self, name: &TubeName) -> Vec<JobId> {
        self.t_tube_by_name(name)
            .map(|t| t.ready.iter().map(|&(_, id)| id).collect())
            .unwrap_or_default()
    }

    pub(crate) fn t_tube_delayed_ids(&self, name: &TubeName) -> Vec<JobId> {
        self.t_tube_by_name(name)
            .map(|t| t.delayed.iter().map(|&(_, id)| id).collect())
            .unwrap_or_default()
    }

    pub(crate) fn t_tube_buried_ids(&self, name: &TubeName) -> Vec<JobId> {
        self.t_tube_by_name(name)
            .map(|t| t.buried.iter().copied().collect())
            .unwrap_or_default()
    }

    pub(crate) fn t_tube_paused(&self, name: &TubeName) -> Option<bool> {
        self.t_tube_by_name(name).map(|t| t.pause > 0)
    }

    pub(crate) fn t_tube_delayed_len(&self, name: &TubeName) -> Option<usize> {
        self.t_tube_by_name(name).map(|t| t.delayed.len())
    }

    pub(crate) fn t_tube_buried_len(&self, name: &TubeName) -> Option<usize> {
        self.t_tube_by_name(name).map(|t| t.buried.len())
    }

    pub(crate) fn t_tube_waiting_conns(&self, name: &TubeName) -> Option<usize> {
        self.t_tube_by_name(name).map(|t| t.waiting_conns.len())
    }

    pub(crate) fn t_tube_exists(&self, name: &TubeName) -> bool {
        self.t_tube_by_name(name).is_some()
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
        self.tube_ids.keys().cloned().collect()
    }

    pub(crate) fn t_all_conn_ids(&self) -> Vec<ConnId> {
        self.conns.keys().copied().collect()
    }

    /// `Some(too_big)` while `cid` has a put in flight.
    pub(crate) fn t_conn_pending_put(&self, cid: ConnId) -> Option<bool> {
        self.conns
            .get(&cid)
            .and_then(|c| c.pending_put)
            .map(|p| p.id.is_none())
    }

    pub(crate) fn t_cur_conns(&self) -> u32 {
        self.cur_conns
    }

    /// The pre-index `next_deadline`: a from-scratch scan of every tube
    /// and connection, as the engine did before T6b.
    pub(crate) fn t_scan_next_deadline(&self) -> Option<Nanos> {
        let mut best: Option<Nanos> = None;
        let mut consider = |d: Nanos| best = Some(best.map_or(d, |b| b.min(d)));
        for &tid in &self.tube_order.items {
            if let Some(t) = self.tube(tid) {
                if let Some(&(d, _)) = t.delayed.first() {
                    consider(d);
                }
                if t.pause > 0 {
                    consider(t.unpause_at);
                }
            }
        }
        for c in self.conns.values() {
            if let Some(t) = conn_tickat(c) {
                consider(t);
            }
        }
        best
    }

    /// The pre-index `soonest_delayed_job`: scan tubes in `tube_order`
    /// order, strict `<` on the deadline.
    fn t_scan_soonest_delayed_job(&self) -> Option<(Nanos, TubeId, JobId)> {
        let mut best: Option<(Nanos, TubeId, JobId)> = None;
        for &tid in &self.tube_order.items {
            if let Some(&(d, id)) = self.tube(tid).and_then(|t| t.delayed.first())
                && best.is_none_or(|(bd, _, _)| d < bd)
            {
                best = Some((d, tid, id));
            }
        }
        best
    }

    /// Asserts that every index equals what a from-scratch recomputation
    /// gives, and that `next_deadline()` equals the scan.
    pub(crate) fn t_check_indexes(&self) {
        if let Err(e) = self.validate() {
            panic!("engine state is invalid: {e}");
        }
        let conn_ticks: BTreeSet<(Nanos, ConnId)> = self
            .conns
            .iter()
            .filter_map(|(&cid, c)| conn_tickat(c).map(|t| (t, cid)))
            .collect();
        assert_eq!(self.conn_ticks, conn_ticks, "conn_ticks index is stale");
        for (&cid, c) in &self.conns {
            assert_eq!(
                c.tick_key,
                conn_tickat(c),
                "tick_key of conn {cid} is stale"
            );
        }

        let mut delay_heads = BTreeSet::new();
        let mut pauses = BTreeSet::new();
        let mut dispatchable = BTreeSet::new();
        let mut delayed_ct = 0;
        let mut live = 0;
        for (tid, t) in self.tubes.iter().enumerate() {
            let Some(t) = t else { continue };
            live += 1;
            assert_eq!(
                self.tube_order.items.get(t.pos),
                Some(&tid),
                "pos of tube {tid}"
            );
            assert_eq!(self.tube_ids.get(&t.name), Some(&tid), "name of tube {tid}");
            if let Some(&(d, _)) = t.delayed.first() {
                delay_heads.insert((d, tid));
            }
            assert_eq!(t.delay_head, t.delayed.first().map(|&(d, _)| d));
            if t.pause > 0 {
                pauses.insert((t.unpause_at, tid));
            }
            let disp = !t.waiting_conns.is_empty() && !t.ready.is_empty();
            if disp {
                dispatchable.insert(tid);
            }
            assert_eq!(t.dispatchable, disp, "dispatchable flag of tube {tid}");
            delayed_ct += t.delayed.len() as u64;
        }
        assert_eq!(live, self.tube_order.len());
        assert_eq!(live, self.tube_ids.len());
        assert_eq!(self.delay_heads, delay_heads, "delay_heads index is stale");
        assert_eq!(self.pauses, pauses, "pauses index is stale");
        assert_eq!(self.dispatchable, dispatchable, "dispatchable set is stale");
        assert_eq!(self.delayed_ct, delayed_ct, "delayed_ct");
        assert_eq!(self.next_deadline(), self.t_scan_next_deadline());
        assert_eq!(
            self.soonest_delayed_job(),
            self.t_scan_soonest_delayed_job()
        );
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use bytes::Bytes;

    use bstk_proto::{Command, PutRejection, Response, TubeName};

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

    // -----------------------------------------------------------------
    // T5 differential-test regressions
    // -----------------------------------------------------------------

    /// prot.c applies `if (delay == 0) delay = 1;` to nanoseconds, so
    /// `pause-tube x 0` pauses for 1 ns: a reserve an instant later is
    /// served, and stats-tube reports `pause: 0`.
    #[test]
    fn pause_tube_zero_pauses_for_one_nanosecond() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        let id = put(&mut e, 0, 1, 0, 0, 60, "x");
        let out = handle(
            &mut e,
            SEC,
            1,
            Command::PauseTube {
                tube: tube("default"),
                delay: 0,
            },
        );
        assert_eq!(only(&out, 1), Response::Paused);
        let stats = e.build_stats_tube(&tube("default"), SEC).unwrap();
        assert_eq!(stats.pause, 0);
        assert_eq!(stats.cmd_pause_tube, 1);
        assert_eq!(e.next_deadline(), Some(SEC + 1));

        let out = handle(&mut e, SEC + 1_000, 1, Command::ReserveWithTimeout(0));
        assert_eq!(
            only(&out, 1),
            Response::Reserved {
                id,
                body: Bytes::from_static(b"x")
            }
        );
    }

    /// An invalid pause-tube name is counted before being rejected.
    #[test]
    fn pause_tube_bad_name_counts_and_replies_bad_format() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        let out = handle(&mut e, 0, 1, Command::PauseTubeBadName);
        assert_eq!(only(&out, 1), Response::BadFormat);
        assert_eq!(e.build_stats_server(0).cmd_pause_tube, 1);
        // No tube stats change: the reference never looked the tube up.
        let stats = e.build_stats_tube(&tube("default"), 0).unwrap();
        assert_eq!(stats.cmd_pause_tube, 0);
    }

    /// connclose runs `ms_clear(&c->watch)`, which deletes index 0 over and
    /// over; tubes are therefore destroyed in the order watch[0],
    /// watch[last], watch[last-1], ... and each destruction swap-removes
    /// from the global tube list.
    #[test]
    fn disconnect_destroys_watched_tubes_in_ms_clear_order() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        e.connect(0, 2);
        handle(&mut e, 0, 1, Command::Watch(tube("a")));
        handle(&mut e, 0, 1, Command::Watch(tube("b")));
        handle(&mut e, 0, 2, Command::Watch(tube("x")));
        handle(&mut e, 0, 2, Command::Watch(tube("y")));
        let names =
            |e: &Engine| -> Vec<String> { e.tube_names().iter().map(|t| t.to_string()).collect() };
        assert_eq!(names(&e), ["default", "a", "b", "x", "y"]);

        let mut out = Outbox::new();
        e.disconnect(0, 1, &mut out);
        // b is destroyed first ([default,a,y,x]), then a ([default,x,y]).
        assert_eq!(names(&e), ["default", "x", "y"]);
    }

    /// reserve-with-timeout 0 while holding a job inside the safety margin,
    /// with the only ready job in a paused tube: the reference starts
    /// waiting (conn_ready ignores pause) and its next conn_timeout checks
    /// "deadline soon" before the timeout, so the reply is DEADLINE_SOON.
    #[test]
    fn reserve_with_timeout_zero_in_margin_with_paused_ready_job_is_deadline_soon() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        handle(&mut e, 0, 1, Command::Use(tube("p")));
        put(&mut e, 0, 1, 0, 0, 60, "paused");
        handle(
            &mut e,
            0,
            1,
            Command::PauseTube {
                tube: tube("p"),
                delay: 10,
            },
        );
        handle(&mut e, 0, 1, Command::Watch(tube("p")));
        handle(&mut e, 0, 1, Command::Use(tube("default")));
        let held = put(&mut e, 0, 1, 0, 0, 1, "short");
        let out = handle(&mut e, 0, 1, Command::Reserve);
        assert!(matches!(only(&out, 1), Response::Reserved { id, .. } if id == held));

        let out = handle(&mut e, 1_000, 1, Command::ReserveWithTimeout(0));
        assert_eq!(only(&out, 1), Response::DeadlineSoon);
        assert!(!e.t_conn_waiting(1));

        // Without a held job in the margin it is still a plain TIMED_OUT.
        e.connect(0, 2);
        handle(&mut e, 0, 2, Command::Watch(tube("p")));
        handle(&mut e, 0, 2, Command::Ignore(tube("default")));
        let out = handle(&mut e, 1_000, 2, Command::ReserveWithTimeout(0));
        assert_eq!(only(&out, 2), Response::TimedOut);
    }

    /// prot.c allocates the job id (make_job) when the put *line* is
    /// parsed, so a put whose body is still in flight owns an id that a
    /// put completed meanwhile on another connection cannot take.
    #[test]
    fn put_started_allocates_the_id_at_header_time() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        e.connect(0, 2);
        e.put_started(0, 1, false);
        let stats = e.build_stats_server(0);
        assert_eq!(stats.cmd_put, 1);
        assert_eq!(stats.current_producers, 1);

        assert_eq!(put(&mut e, 0, 2, 0, 0, 60, "second"), 2);
        let out = handle(
            &mut e,
            0,
            1,
            Command::Put {
                pri: 0,
                delay: 0,
                ttr: 60,
                body: Bytes::from_static(b"first"),
            },
        );
        assert_eq!(only(&out, 1), Response::Inserted(1));
        let stats = e.build_stats_server(0);
        assert_eq!(stats.cmd_put, 2);
        assert_eq!(stats.total_jobs, 2);
    }

    /// A connection that closes mid-body has still been counted and has
    /// consumed its id (job_free of `c->in_job` in connclose).
    #[test]
    fn put_started_then_disconnect_consumes_the_id() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        e.connect(0, 2);
        e.put_started(0, 1, false);
        let mut out = Outbox::new();
        e.disconnect(0, 1, &mut out);
        assert!(out.is_empty());
        let stats = e.build_stats_server(0);
        assert_eq!(stats.cmd_put, 1);
        assert_eq!(stats.current_producers, 0);
        assert_eq!(put(&mut e, 0, 2, 0, 0, 60, "x"), 2);
    }

    /// Completion rejections after `put_started` do not repeat the
    /// header-time side effects.
    #[test]
    fn put_started_then_rejection_counts_once() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        e.put_started(0, 1, true);
        assert_eq!(e.build_stats_server(0).current_producers, 0);
        let mut out = Outbox::new();
        e.put_rejected(0, 1, PutRejection::JobTooBig, &mut out);
        assert_eq!(only(&out, 1), Response::JobTooBig);

        e.put_started(0, 1, false);
        let mut out = Outbox::new();
        e.put_rejected(0, 1, PutRejection::ExpectedCrlf, &mut out);
        assert_eq!(only(&out, 1), Response::ExpectedCrlf);

        let stats = e.build_stats_server(0);
        assert_eq!(stats.cmd_put, 2);
        assert_eq!(stats.current_producers, 1);
        // Only the EXPECTED_CRLF put consumed an id.
        assert_eq!(put(&mut e, 0, 1, 0, 0, 60, "x"), 2);
    }

    /// Drain mode is checked at body completion, after the header-time id
    /// allocation: the id is consumed exactly once.
    #[test]
    fn put_started_while_draining_consumes_one_id() {
        let mut e = engine_at(0);
        e.connect(0, 1);
        e.set_draining(true);
        e.put_started(0, 1, false);
        let out = handle(
            &mut e,
            0,
            1,
            Command::Put {
                pri: 0,
                delay: 0,
                ttr: 60,
                body: Bytes::from_static(b"x"),
            },
        );
        assert_eq!(only(&out, 1), Response::Draining);
        e.set_draining(false);
        assert_eq!(put(&mut e, 0, 1, 0, 0, 60, "y"), 2);
        assert_eq!(e.build_stats_server(0).cmd_put, 2);
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
// `Engine::snapshot` (P2-T2): equals what `stats` / `stats-tube` /
// `list-tubes` report at the same `now`, and never changes state.
// -----------------------------------------------------------------------
#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod snapshot_tests {
    use bytes::Bytes;

    use bstk_proto::{Command, Response, StatsServer, TubeName};

    use crate::{ConnId, EngineConfig, Nanos, Outbox, StaticSysInfo, SysSnapshot};

    use super::Engine;

    const SEC: Nanos = crate::NANOS_PER_SEC;
    /// Connection used only to issue the introspection commands.
    const OBSERVER: ConnId = 99;

    fn tube(name: &str) -> TubeName {
        TubeName::new(name).unwrap()
    }

    fn engine(journal: bool) -> Engine {
        let sys = SysSnapshot {
            pid: 4242,
            version: "1.13-test".into(),
            rusage_utime: (3, 250_000),
            rusage_stime: (1, 7),
            id: "abcdef0123456789".into(),
            hostname: "host".into(),
            os: "os".into(),
            platform: "plat".into(),
        };
        let mut e = Engine::new(
            0,
            EngineConfig {
                journal,
                ..EngineConfig::default()
            },
            Box::new(StaticSysInfo(sys)),
        );
        e.connect(0, OBSERVER);
        e
    }

    fn run(e: &mut Engine, now: Nanos, cid: ConnId, cmd: Command) -> Outbox {
        let mut out = Outbox::new();
        e.handle(now, cid, cmd, &mut out);
        out
    }

    fn put(e: &mut Engine, now: Nanos, cid: ConnId, pri: u32, delay: u32, ttr: u32) -> u64 {
        let out = run(
            e,
            now,
            cid,
            Command::Put {
                pri,
                delay,
                ttr,
                body: Bytes::from_static(b"body"),
            },
        );
        match out.as_slice() {
            [(c, Response::Inserted(id))] if *c == cid => *id,
            other => panic!("expected Inserted, got {other:?}"),
        }
    }

    fn only_reply(out: &Outbox) -> Response {
        match out.as_slice() {
            [(c, r)] if *c == OBSERVER => r.clone(),
            other => panic!("expected one reply to the observer, got {other:?}"),
        }
    }

    /// Takes a snapshot at `now` and checks it against the wire commands:
    /// the snapshot itself must leave `stats` untouched, `stats` must
    /// report the snapshot plus its own `cmd-stats` increment, and
    /// `list-tubes` / `stats-tube` must match `tubes`.
    fn check_snapshot(e: &mut Engine, now: Nanos) {
        let mut journal = Vec::new();
        e.take_journal(&mut journal);
        journal.clear();
        let before = e.build_stats_server(now);

        let snap = e.snapshot(now);
        assert_eq!(snap.server, before, "snapshot.server != stats at now");
        assert_eq!(e.build_stats_server(now), before, "snapshot changed stats");
        e.take_journal(&mut journal);
        assert!(journal.is_empty(), "snapshot journaled {journal:?}");
        // Deterministic: a second snapshot is identical.
        assert_eq!(e.snapshot(now), snap);

        // `stats` = snapshot + exactly its own cmd-stats increment.
        let reply = only_reply(&run(e, now, OBSERVER, Command::Stats));
        let mut expected = snap.server.clone();
        expected.cmd_stats += 1;
        assert_eq!(reply, Response::Ok(expected.to_yaml()));
        assert_eq!(
            StatsServer {
                cmd_stats: before.cmd_stats,
                ..e.build_stats_server(now)
            },
            before,
            "stats changed something besides cmd-stats"
        );

        // Tube order and content.
        let reply = only_reply(&run(e, now, OBSERVER, Command::ListTubes));
        let names: Vec<TubeName> = snap.tubes.iter().map(|t| t.name.clone()).collect();
        assert_eq!(reply, Response::Ok(bstk_proto::yaml_list(names.iter())));
        assert_eq!(names, e.tube_names());
        assert_eq!(snap.server.current_tubes, snap.tubes.len() as u64);
        for t in &snap.tubes {
            let reply = only_reply(&run(e, now, OBSERVER, Command::StatsTube(t.name.clone())));
            assert_eq!(reply, Response::Ok(t.to_yaml()));
        }
    }

    /// A state with jobs in every state, a paused tube, a waiting
    /// reserver and a tube order that is not creation order (a destroyed
    /// tube was swap-removed).
    fn busy_state(journal: bool) -> Engine {
        let mut e = engine(journal);
        for c in 1..=3 {
            e.connect(0, c);
        }
        // Creates tubes a, b, c (watched by conn 2), then drops `a` so `c`
        // moves into its slot: list order becomes default, c, b.
        for name in ["a", "b", "c"] {
            run(&mut e, 0, 2, Command::Watch(tube(name)));
        }
        run(&mut e, 0, 2, Command::Ignore(tube("a")));
        run(&mut e, 0, 1, Command::Use(tube("b")));
        let urgent = put(&mut e, 0, 1, 5, 0, 10);
        let _ready = put(&mut e, 0, 1, 2000, 0, 10);
        let _delayed = put(&mut e, 0, 1, 100, 30, 10);
        let to_bury = put(&mut e, 0, 1, 100, 0, 10);
        run(&mut e, 0, 1, Command::ReserveJob(to_bury));
        run(
            &mut e,
            0,
            1,
            Command::Bury {
                id: to_bury,
                pri: 1,
            },
        );
        run(&mut e, 0, 1, Command::ReserveJob(urgent));
        run(&mut e, 0, 1, Command::Use(tube("c")));
        let doomed = put(&mut e, 0, 1, 0, 0, 10);
        run(&mut e, 0, 1, Command::Delete(doomed));
        run(
            &mut e,
            0,
            OBSERVER,
            Command::PauseTube {
                tube: tube("c"),
                delay: 20,
            },
        );
        // conn 3 waits on `default`, which is empty.
        let out = run(&mut e, 0, 3, Command::ReserveWithTimeout(100));
        assert!(out.is_empty());
        e
    }

    #[test]
    fn snapshot_of_fresh_engine() {
        for journal in [false, true] {
            let mut e = engine(journal);
            let snap = e.snapshot(0);
            assert_eq!(snap.tubes.len(), 1);
            assert_eq!(snap.tubes[0].name, tube("default"));
            assert_eq!(snap.server.cmd_stats, 0);
            assert_eq!(snap.server.pid, 4242);
            assert_eq!(snap.server.rusage_utime, (3, 250_000));
            check_snapshot(&mut e, 0);
        }
    }

    #[test]
    fn snapshot_matches_stats_across_states_and_times() {
        for journal in [false, true] {
            let mut e = busy_state(journal);
            let snap = e.snapshot(0);
            let names: Vec<&str> = snap.tubes.iter().map(|t| t.name.as_str()).collect();
            assert_eq!(names, ["default", "c", "b"]);
            assert_eq!(snap.server.current_jobs_urgent, 0);
            assert_eq!(snap.server.current_jobs_ready, 1);
            assert_eq!(snap.server.current_jobs_reserved, 1);
            assert_eq!(snap.server.current_jobs_delayed, 1);
            assert_eq!(snap.server.current_jobs_buried, 1);
            assert_eq!(snap.server.current_waiting, 1);
            assert_eq!(snap.tubes[1].pause, 20);
            assert_eq!(snap.tubes[1].pause_time_left, 20);
            assert_eq!(snap.tubes[1].cmd_delete, 1);
            assert_eq!(snap.tubes[1].cmd_pause_tube, 1);

            // Same state observed at several times, with ticks in between
            // (TTR expiry, delay expiry, pause expiry, reserve timeout).
            let mut out = Outbox::new();
            for now in [0, SEC / 2, 5 * SEC, 11 * SEC, 25 * SEC, 31 * SEC, 200 * SEC] {
                e.tick(now, &mut out);
                check_snapshot(&mut e, now);
            }
            // A binlog stats push and drain mode are reflected too.
            e.set_binlog_stats(crate::BinlogStats {
                oldest_index: 2,
                current_index: 5,
                records_written: 17,
                records_migrated: 3,
            });
            e.set_draining(true);
            let snap = e.snapshot(200 * SEC);
            assert!(snap.server.draining);
            assert_eq!(snap.server.binlog_records_written, 17);
            check_snapshot(&mut e, 200 * SEC);
            // Disconnecting everyone collapses the tube list again.
            for c in 1..=3 {
                e.disconnect(200 * SEC, c, &mut out);
            }
            check_snapshot(&mut e, 201 * SEC);
        }
    }

    /// `snapshot_limited` is a prefix of `snapshot` (in `list-tubes`
    /// order) with the same, complete server stats, and is just as pure.
    fn check_snapshot_limited(e: &mut Engine, now: Nanos) {
        let full = e.snapshot(now);
        let before = e.build_stats_server(now);
        for k in 0..=full.tubes.len() + 2 {
            let lim = e.snapshot_limited(now, k);
            assert_eq!(lim.server, full.server, "server stats with limit {k}");
            assert_eq!(
                lim.server.current_tubes,
                full.tubes.len() as u64,
                "current_tubes stays the full count"
            );
            let n = k.min(full.tubes.len());
            assert_eq!(lim.tubes, full.tubes[..n], "tubes with limit {k}");
        }
        assert_eq!(e.snapshot_limited(now, usize::MAX), full);
        assert_eq!(
            e.build_stats_server(now),
            before,
            "snapshot_limited changed stats"
        );
        let mut journal = Vec::new();
        e.take_journal(&mut journal);
        assert!(journal.is_empty(), "snapshot_limited journaled {journal:?}");
    }

    #[test]
    fn snapshot_limited_of_fresh_engine() {
        for journal in [false, true] {
            let mut e = engine(journal);
            let snap = e.snapshot_limited(0, 0);
            assert!(snap.tubes.is_empty());
            assert_eq!(snap.server.current_tubes, 1);
            assert_eq!(snap.server.pid, 4242);
            check_snapshot_limited(&mut e, 0);
        }
    }

    #[test]
    fn snapshot_limited_is_a_prefix_across_states_and_times() {
        for journal in [false, true] {
            let mut e = busy_state(journal);
            let mut drained = Vec::new();
            e.take_journal(&mut drained);
            let names: Vec<String> = e
                .snapshot_limited(0, 2)
                .tubes
                .iter()
                .map(|t| t.name.as_str().to_owned())
                .collect();
            // List order, not creation order (see `busy_state`).
            assert_eq!(names, ["default", "c"]);
            let mut out = Outbox::new();
            for now in [0, SEC / 2, 11 * SEC, 25 * SEC, 200 * SEC] {
                e.tick(now, &mut out);
                e.take_journal(&mut drained);
                check_snapshot_limited(&mut e, now);
                check_snapshot(&mut e, now);
            }
        }
    }

    #[test]
    fn snapshot_limited_bounds_many_tubes() {
        let mut e = engine(false);
        e.connect(0, 1);
        for i in 0..50 {
            run(&mut e, 0, 1, Command::Watch(tube(&format!("t{i}"))));
        }
        let full = e.snapshot(SEC);
        assert_eq!(full.tubes.len(), 51);
        let lim = e.snapshot_limited(SEC, 5);
        assert_eq!(lim.tubes.len(), 5);
        assert_eq!(lim.server.current_tubes, 51);
        assert_eq!(lim.tubes, full.tubes[..5]);
        check_snapshot_limited(&mut e, SEC);
        // Taking it counts as nothing either.
        let s = e.build_stats_server(SEC);
        assert_eq!((s.cmd_stats, s.cmd_stats_tube, s.cmd_list_tubes), (0, 0, 0));
    }

    #[test]
    fn snapshot_does_not_count_as_a_command() {
        let e = busy_state(false);
        for _ in 0..5 {
            let _ = e.snapshot(SEC);
        }
        let s = e.build_stats_server(SEC);
        assert_eq!(
            (
                s.cmd_stats,
                s.cmd_stats_tube,
                s.cmd_list_tubes,
                s.cmd_stats_job
            ),
            (0, 0, 0, 0)
        );
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
pub(crate) mod proptests {
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

    pub(crate) fn check_invariants(e: &Engine) {
        // (0) every deadline index equals a from-scratch recomputation.
        e.t_check_indexes();

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

    /// `snapshot` equals the stats builders and leaves them (and the
    /// journal) untouched. Expects the journal to have just been drained.
    fn check_snapshot_is_pure(e: &mut Engine, now: Nanos) {
        let before = e.build_stats_server(now);
        let snap = e.snapshot(now);
        assert_eq!(snap.server, before);
        let names: Vec<TubeName> = snap.tubes.iter().map(|t| t.name.clone()).collect();
        assert_eq!(names, e.tube_names());
        for t in &snap.tubes {
            assert_eq!(e.build_stats_tube(&t.name, now).as_ref(), Some(t));
        }
        assert_eq!(e.build_stats_server(now), before);
        let mut journal = Vec::new();
        e.take_journal(&mut journal);
        assert!(journal.is_empty(), "snapshot journaled {journal:?}");
    }

    fn run_actions(actions: Vec<Action>, journal: bool) {
        let mut e = Engine::new(
            0,
            EngineConfig {
                journal,
                ..EngineConfig::default()
            },
            Box::new(StaticSysInfo::default()),
        );
        let mut journal_buf = Vec::new();
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
            journal_buf.clear();
            e.take_journal(&mut journal_buf);
            assert!(journal || journal_buf.is_empty());
            check_invariants(&e);
            check_snapshot_is_pure(&mut e, now);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 10_000, ..ProptestConfig::default() })]

        #[test]
        fn state_machine_invariants_hold(actions in prop::collection::vec(action_strategy(), 15..35)) {
            run_actions(actions, false);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 4_000, ..ProptestConfig::default() })]

        #[test]
        fn state_machine_invariants_hold_with_journal(actions in prop::collection::vec(action_strategy(), 15..35)) {
            run_actions(actions, true);
        }
    }
}
