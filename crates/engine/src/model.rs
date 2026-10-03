//! In-memory data types for jobs, tubes and connections. Mirrors the shape
//! of `Jobrec`/`Job`/`Tube`/`Conn` in `.ref/beanstalkd/dat.h`, minus the
//! WAL/socket/IO bookkeeping fields that don't apply to a pure state
//! machine.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;

use bstk_proto::{JobId, TubeName};

use crate::ms::Ms;
use crate::{ConnId, Nanos};

/// Index of a live tube in `Engine::tubes` (a slab). Slots are reused only
/// after the tube is destroyed, and every index entry naming a tube is
/// removed when it is destroyed.
pub(crate) type TubeId = usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum JobState {
    Ready,
    Reserved,
    Delayed,
    Buried,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct JobRec {
    pub(crate) id: JobId,
    pub(crate) tube: TubeId,
    pub(crate) pri: u32,
    /// Original delay in whole seconds, as last set by `put` or `release`.
    pub(crate) delay: u32,
    /// TTR in whole seconds, already bumped from 0 to 1 if needed.
    pub(crate) ttr: u32,
    pub(crate) body: Bytes,
    pub(crate) created_at: Nanos,
    /// Meaningful only while `state` is `Delayed` (time to become ready) or
    /// `Reserved` (TTR deadline).
    pub(crate) deadline_at: Nanos,
    pub(crate) state: JobState,
    pub(crate) reserver: Option<ConnId>,
    /// This job's key in its current list: `TubeState::buried` while
    /// `state` is `Buried`, `ConnState::reserved` while `Reserved`. A job
    /// is never in both, so one field suffices (the reference makes the
    /// same trade with a single shared `prev`/`next` pair). Meaningful
    /// only in those two states; stale otherwise, like `deadline_at`.
    pub(crate) list_seq: u64,
    pub(crate) reserve_ct: u32,
    pub(crate) timeout_ct: u32,
    pub(crate) release_ct: u32,
    pub(crate) bury_ct: u32,
    pub(crate) kick_ct: u32,
    /// Reported as `file` by stats-job: the binlog `current_index` known to
    /// the engine when the job's first journal record was produced (0 when
    /// journaling is off, and for recovered jobs). See `Engine::cmd_put`.
    pub(crate) file: u64,
}

impl JobRec {
    pub(crate) fn state_name(&self) -> &'static str {
        match self.state {
            JobState::Ready => "ready",
            JobState::Reserved => "reserved",
            JobState::Delayed => "delayed",
            JobState::Buried => "buried",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct TubeStat {
    /// Ready jobs with pri < URGENT_THRESHOLD (ready-only, per prot.c).
    pub(crate) urgent_ct: u64,
    pub(crate) buried_ct: u64,
    pub(crate) reserved_ct: u64,
    pub(crate) waiting_ct: u64,
    pub(crate) pause_ct: u64,
    pub(crate) total_delete_ct: u64,
    pub(crate) total_jobs_ct: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct TubeState {
    pub(crate) name: TubeName,
    pub(crate) ready: BTreeSet<(u32, JobId)>,
    pub(crate) delayed: BTreeSet<(Nanos, JobId)>,
    /// Keyed by `Engine::next_list_seq` at insertion time (iteration order
    /// is then FIFO, matching the reference). An index-linked list, like
    /// the reference's `prev`/`next`, would be O(1) here, but every
    /// untrusted snapshot would then need cycle and link-symmetry checks;
    /// this is O(log n), like the neighboring `ready`/`delayed` sets, and
    /// `Engine::validate` checks it the same way (key equals `list_seq`).
    pub(crate) buried: BTreeMap<u64, JobId>,
    pub(crate) waiting_conns: Ms<ConnId>,
    /// Reference counting inputs. A tube is alive while
    /// `using_ct + watching_ct + job_ref_ct > 0`, except "default" which is
    /// immortal (it holds a permanent reference via the static
    /// `default_tube` pointer in the reference implementation).
    pub(crate) using_ct: u32,
    pub(crate) watching_ct: u32,
    pub(crate) job_ref_ct: u64,
    pub(crate) stat: TubeStat,
    /// 0 means "not paused"; otherwise the pause duration in nanoseconds.
    /// While non-zero, `(unpause_at, id)` is in `Engine::pauses`.
    pub(crate) pause: Nanos,
    pub(crate) unpause_at: Nanos,
    pub(crate) pos: usize,
    /// Deadline of the head of `delayed` as currently recorded in
    /// `Engine::delay_heads`.
    pub(crate) delay_head: Option<Nanos>,
    /// Whether this tube is in `Engine::dispatchable` (it has both waiting
    /// connections and ready jobs).
    pub(crate) dispatchable: bool,
}

impl TubeState {
    pub(crate) fn new(name: TubeName, pos: usize) -> Self {
        TubeState {
            name,
            ready: BTreeSet::new(),
            delayed: BTreeSet::new(),
            buried: BTreeMap::new(),
            waiting_conns: Ms::new(),
            using_ct: 0,
            watching_ct: 0,
            job_ref_ct: 0,
            stat: TubeStat::default(),
            pause: 0,
            unpause_at: 0,
            pos,
            delay_head: None,
            dispatchable: false,
        }
    }

    pub(crate) fn refs(&self) -> u64 {
        self.using_ct as u64 + self.watching_ct as u64 + self.job_ref_ct
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct ConnState {
    pub(crate) use_tube: TubeId,
    pub(crate) watch: Ms<TubeId>,
    pub(crate) is_producer: bool,
    pub(crate) is_worker: bool,
    pub(crate) waiting: bool,
    /// Absolute deadline for an explicit `reserve-with-timeout`; `None`
    /// means either not waiting, or waiting with no timeout (plain
    /// `reserve`).
    pub(crate) wait_deadline: Option<Nanos>,
    /// Reservation order (oldest first), used when releasing all of this
    /// connection's jobs on disconnect. Same `list_seq`-keyed shape as
    /// `TubeState::buried`, for the same reason: O(log n) removal of an
    /// arbitrary job (release/delete need not target the oldest one).
    pub(crate) reserved: BTreeMap<u64, JobId>,
    pub(crate) reserved_by_deadline: BTreeSet<(Nanos, JobId)>,
    /// A put whose command line was accepted (`Engine::put_started`) but
    /// whose body has not completed yet: prot.c's `c->in_job`.
    pub(crate) pending_put: Option<PendingPut>,
    /// This connection's `conntickat` as currently recorded in
    /// `Engine::conn_ticks`.
    pub(crate) tick_key: Option<Nanos>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct PendingPut {
    /// The job id `make_job` already allocated; `None` for an oversized
    /// put, which the reference only counts before discarding its body.
    pub(crate) id: Option<JobId>,
    pub(crate) created_at: Nanos,
}

impl ConnState {
    pub(crate) fn new(default_tube: TubeId) -> Self {
        let mut watch = Ms::new();
        watch.append(default_tube);
        ConnState {
            use_tube: default_tube,
            watch,
            is_producer: false,
            is_worker: false,
            waiting: false,
            wait_deadline: None,
            reserved: BTreeMap::new(),
            reserved_by_deadline: BTreeSet::new(),
            pending_put: None,
            tick_key: None,
        }
    }

    pub(crate) fn soonest_reserved(&self) -> Option<(Nanos, JobId)> {
        self.reserved_by_deadline.iter().next().copied()
    }
}
