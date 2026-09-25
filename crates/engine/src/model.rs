//! In-memory data types for jobs, tubes and connections. Mirrors the shape
//! of `Jobrec`/`Job`/`Tube`/`Conn` in `.ref/beanstalkd/dat.h`, minus the
//! WAL/socket/IO bookkeeping fields that don't apply to a pure state
//! machine.

use std::collections::{BTreeSet, VecDeque};

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
    /// FIFO order (oldest-buried first), matching the reference's
    /// doubly-linked list with tail insertion.
    pub(crate) buried: VecDeque<JobId>,
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
    /// Position in `Engine::tube_order` (kept in sync on swap-removal).
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
            buried: VecDeque::new(),
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
    /// connection's jobs on disconnect.
    pub(crate) reserved_fifo: Vec<JobId>,
    /// Same set, ordered by TTR deadline for fast "soonest" lookups.
    pub(crate) reserved_by_deadline: BTreeSet<(Nanos, JobId)>,
    /// A put whose command line was accepted (`Engine::put_started`) but
    /// whose body has not completed yet: prot.c's `c->in_job`.
    pub(crate) pending_put: Option<PendingPut>,
    /// This connection's `conntickat` as currently recorded in
    /// `Engine::conn_ticks`.
    pub(crate) tick_key: Option<Nanos>,
}

/// Header-time state of an in-flight put (see `ConnState::pending_put`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct PendingPut {
    /// The job id `make_job` already allocated; `None` for an oversized
    /// put, which the reference only counts before discarding its body.
    pub(crate) id: Option<JobId>,
    /// `created_at` as set by `make_job` at header time.
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
            reserved_fifo: Vec::new(),
            reserved_by_deadline: BTreeSet::new(),
            pending_put: None,
            tick_key: None,
        }
    }

    pub(crate) fn soonest_reserved(&self) -> Option<(Nanos, JobId)> {
        self.reserved_by_deadline.iter().next().copied()
    }
}
