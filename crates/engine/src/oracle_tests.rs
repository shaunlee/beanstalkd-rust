//! Differential test against the frozen pre-T6b engine
//! (`crates/engine-oracle`). Both engines receive identical `(now, message)`
//! sequences; after every step their outboxes, `next_deadline()`, server
//! stats, per-tube stats, per-job stats, tube order and watch lists must be
//! identical, and every index of the new engine must equal a from-scratch
//! recomputation (including `next_deadline()` vs. a full scan).
//!
//! Time moves in whole seconds, single nanoseconds, and jumps to exactly
//! `next_deadline() - 1 ns`, `+ 0` and `+ 1 ns`, so TTR expiry (`<`), the
//! DEADLINE_SOON margin (`>=`), delays, `pause-tube 0` (1 ns) and reserve
//! timeouts are all hit on their exact boundaries.
//!
//! The new engine runs both with journaling off and on (the oracle has no
//! journal): journaling must not change any reply or statistic. With it off,
//! `take_journal` must always yield nothing.

#![allow(clippy::unwrap_used)]

use std::collections::HashSet;

use bytes::Bytes;
use proptest::prelude::*;

use bstk_engine_oracle as oracle;
use bstk_proto::{Command, JobId, PutRejection, TubeName};

use crate::{
    ConnId, Engine, EngineConfig, JournalEntry, NANOS_PER_SEC, Nanos, Outbox, StaticSysInfo,
};

const SEC: Nanos = NANOS_PER_SEC;
const TUBES: [&str; 4] = ["default", "a", "b", "c"];
/// Connection ids `0..CONNS`; the first `PRECONNECTED` connect up front.
const CONNS: ConnId = 6;
const PRECONNECTED: ConnId = 3;
/// Commands address job ids `0..MAX_JOB` (0 never exists).
pub(crate) const MAX_JOB: JobId = 24;

fn tube_name(i: usize) -> TubeName {
    TubeName::new(TUBES[i % TUBES.len()]).unwrap()
}

/// How a put completes (see `Frame::PutStarted` / `Frame::PutRejected`).
#[derive(Debug, Clone, Copy)]
pub(crate) enum PutEnd {
    Body,
    ExpectedCrlf,
    TrailingGarbage,
    TooBig,
}

#[derive(Debug, Clone)]
pub(crate) enum Msg {
    Connect(ConnId),
    Disconnect(ConnId),
    HalfClose(ConnId),
    PutStarted {
        conn: ConnId,
        too_big: bool,
    },
    Put {
        conn: ConnId,
        pri: u32,
        delay: u32,
        ttr: u32,
        end: PutEnd,
    },
    Cmd(ConnId, Command),
    Tick,
    AdvanceNs(Nanos),
    Advance(Nanos),
    /// Jump to `next_deadline() + offset` nanoseconds (if in the future).
    AdvanceToDeadline(i64),
    SetDraining(bool),
}

fn pri() -> impl Strategy<Value = u32> {
    prop_oneof![Just(0u32), Just(1023), Just(1024), 0..2000u32]
}

fn command() -> impl Strategy<Value = Command> {
    let tube = || (0..TUBES.len()).prop_map(tube_name);
    let id = || 0..MAX_JOB;
    let secs = || 0..4u32;
    prop_oneof![
        4 => tube().prop_map(Command::Use),
        3 => tube().prop_map(Command::Watch),
        2 => tube().prop_map(Command::Ignore),
        6 => Just(Command::Reserve),
        5 => secs().prop_map(Command::ReserveWithTimeout),
        2 => id().prop_map(Command::ReserveJob),
        5 => id().prop_map(Command::Delete),
        3 => (id(), pri(), secs()).prop_map(|(id, pri, delay)| Command::Release { id, pri, delay }),
        2 => (id(), pri()).prop_map(|(id, pri)| Command::Bury { id, pri }),
        3 => id().prop_map(Command::Touch),
        1 => id().prop_map(Command::Peek),
        1 => Just(Command::PeekReady),
        1 => Just(Command::PeekDelayed),
        1 => Just(Command::PeekBuried),
        2 => (0..4u32).prop_map(Command::Kick),
        2 => id().prop_map(Command::KickJob),
        1 => id().prop_map(Command::StatsJob),
        1 => tube().prop_map(Command::StatsTube),
        1 => Just(Command::Stats),
        1 => Just(Command::ListTubes),
        1 => Just(Command::ListTubeUsed),
        1 => Just(Command::ListTubesWatched),
        3 => (tube(), 0..3u32).prop_map(|(tube, delay)| Command::PauseTube { tube, delay }),
        1 => Just(Command::PauseTubeBadName),
    ]
}

fn msg() -> impl Strategy<Value = Msg> {
    let conn = || 0..CONNS;
    let end = prop_oneof![
        6 => Just(PutEnd::Body),
        1 => Just(PutEnd::ExpectedCrlf),
        1 => Just(PutEnd::TrailingGarbage),
        1 => Just(PutEnd::TooBig),
    ];
    prop_oneof![
        2 => conn().prop_map(Msg::Connect),
        1 => conn().prop_map(Msg::Disconnect),
        1 => conn().prop_map(Msg::HalfClose),
        2 => (conn(), prop::bool::weighted(0.2))
            .prop_map(|(conn, too_big)| Msg::PutStarted { conn, too_big }),
        7 => (conn(), pri(), 0..3u32, 0..3u32, end).prop_map(|(conn, pri, delay, ttr, end)| {
            Msg::Put { conn, pri, delay, ttr, end }
        }),
        22 => (conn(), command()).prop_map(|(c, cmd)| Msg::Cmd(c, cmd)),
        2 => Just(Msg::Tick),
        2 => (1..=3u64).prop_map(Msg::AdvanceNs),
        3 => prop_oneof![
            Just(SEC / 2),
            Just(SEC - 1),
            Just(SEC),
            Just(SEC + 1),
            Just(2 * SEC),
            Just(5 * SEC),
        ]
        .prop_map(Msg::Advance),
        5 => (-1..=1i64).prop_map(Msg::AdvanceToDeadline),
        1 => prop::bool::weighted(0.3).prop_map(Msg::SetDraining),
    ]
}

/// A message plus whether the caller ticks right after it (the server
/// always does; skipping it covers a message that arrives before an
/// overdue timer has fired).
pub(crate) fn step() -> impl Strategy<Value = (Msg, bool)> {
    (msg(), prop::bool::weighted(0.8))
}

pub(crate) struct Pair {
    pub(crate) new: Engine,
    journal: bool,
    /// The entries the last `run` drained from `new`.
    pub(crate) journal_buf: Vec<JournalEntry>,
    old: oracle::Engine,
    pub(crate) now: Nanos,
    connected: HashSet<ConnId>,
    ever_connected: HashSet<ConnId>,
    waiting: HashSet<ConnId>,
    /// Connections with a put in flight: `Some(too_big)`.
    pending_put: std::collections::HashMap<ConnId, bool>,
}

impl Pair {
    pub(crate) fn new(journal: bool) -> Self {
        let mut p = Pair {
            new: Engine::new(
                0,
                EngineConfig {
                    journal,
                    ..EngineConfig::default()
                },
                Box::new(StaticSysInfo::default()),
            ),
            journal,
            journal_buf: Vec::new(),
            old: oracle::Engine::new(
                0,
                oracle::EngineConfig::default(),
                Box::new(oracle::StaticSysInfo::default()),
            ),
            now: 0,
            connected: HashSet::new(),
            ever_connected: HashSet::new(),
            waiting: HashSet::new(),
            pending_put: std::collections::HashMap::new(),
        };
        for c in 0..PRECONNECTED {
            p.connect(c);
        }
        p
    }

    fn connect(&mut self, c: ConnId) {
        // Connection ids are never reused.
        if self.ever_connected.insert(c) {
            self.new.connect(self.now, c);
            self.old.connect(self.now, c);
            self.connected.insert(c);
        }
    }

    /// Whether the server would send `c` another frame now.
    fn can_send(&self, c: ConnId) -> bool {
        !self.waiting.contains(&c) && !self.pending_put.contains_key(&c)
    }

    pub(crate) fn run(&mut self, msg: Msg, tick_after: bool) {
        let mut out_new = Outbox::new();
        let mut out_old = Outbox::new();
        let now = self.now;
        let mut reserving: Option<ConnId> = None;
        let explicit_tick = matches!(msg, Msg::Tick);
        match msg {
            Msg::Connect(c) => self.connect(c),
            Msg::Disconnect(c) => {
                self.new.disconnect(now, c, &mut out_new);
                self.old.disconnect(now, c, &mut out_old);
                self.connected.remove(&c);
                self.waiting.remove(&c);
                self.pending_put.remove(&c);
            }
            Msg::HalfClose(c) => {
                self.new.half_close(now, c, &mut out_new);
                self.old.half_close(now, c, &mut out_old);
            }
            Msg::PutStarted { conn, too_big } => {
                if self.can_send(conn) {
                    self.new.put_started(now, conn, too_big);
                    self.old.put_started(now, conn, too_big);
                    if self.connected.contains(&conn) {
                        self.pending_put.insert(conn, too_big);
                    }
                }
            }
            Msg::Put {
                conn,
                pri,
                delay,
                ttr,
                end,
            } => {
                if !self.waiting.contains(&conn) {
                    let end = match self.pending_put.remove(&conn) {
                        Some(true) => PutEnd::TooBig,
                        // After PutStarted only EXPECTED_CRLF can reject a
                        // put that fits.
                        Some(false) => match end {
                            PutEnd::Body | PutEnd::TooBig => PutEnd::Body,
                            PutEnd::ExpectedCrlf | PutEnd::TrailingGarbage => PutEnd::ExpectedCrlf,
                        },
                        None => end,
                    };
                    let rejection = match end {
                        PutEnd::Body => None,
                        PutEnd::ExpectedCrlf => Some(PutRejection::ExpectedCrlf),
                        PutEnd::TrailingGarbage => Some(PutRejection::TrailingGarbage),
                        PutEnd::TooBig => Some(PutRejection::JobTooBig),
                    };
                    match rejection {
                        None => {
                            let cmd = Command::Put {
                                pri,
                                delay,
                                ttr,
                                body: Bytes::from_static(b"body"),
                            };
                            self.new.handle(now, conn, cmd.clone(), &mut out_new);
                            self.old.handle(now, conn, cmd, &mut out_old);
                        }
                        Some(why) => {
                            self.new.put_rejected(now, conn, why, &mut out_new);
                            self.old.put_rejected(now, conn, why, &mut out_old);
                        }
                    }
                }
            }
            Msg::Cmd(c, cmd) => {
                if self.can_send(c) {
                    if matches!(cmd, Command::Reserve | Command::ReserveWithTimeout(_)) {
                        reserving = Some(c);
                    }
                    self.new.handle(now, c, cmd.clone(), &mut out_new);
                    self.old.handle(now, c, cmd, &mut out_old);
                }
            }
            Msg::Tick => {}
            Msg::AdvanceNs(dt) | Msg::Advance(dt) => self.now += dt,
            Msg::AdvanceToDeadline(offset) => {
                if let Some(d) = self.new.next_deadline() {
                    let target = d as i128 + offset as i128;
                    if target > self.now as i128 {
                        self.now = target as Nanos;
                    }
                }
            }
            Msg::SetDraining(on) => {
                self.new.set_draining(on);
                self.old.set_draining(on);
            }
        }
        if tick_after || explicit_tick {
            self.new.tick(self.now, &mut out_new);
            self.old.tick(self.now, &mut out_old);
        }
        assert_eq!(out_new, out_old, "outbox differs at now={}", self.now);

        // Drain like the server does after every call.
        self.journal_buf.clear();
        self.new.take_journal(&mut self.journal_buf);
        if !self.journal {
            assert!(self.journal_buf.is_empty(), "journal off but entries");
        }

        for (c, _) in &out_new {
            self.waiting.remove(c);
        }
        if let Some(c) = reserving
            && self.connected.contains(&c)
            && !out_new.iter().any(|(rc, _)| *rc == c)
        {
            self.waiting.insert(c);
        }
        self.compare();
    }

    fn compare(&self) {
        let now = self.now;
        assert_eq!(
            self.new.next_deadline(),
            self.old.next_deadline(),
            "next_deadline"
        );
        self.new.t_check_indexes();
        assert_eq!(
            self.new.build_stats_server(now),
            self.old.build_stats_server(now),
            "server stats"
        );
        let names = self.new.tube_names();
        assert_eq!(names, self.old.tube_names(), "tube order");
        for name in &names {
            assert_eq!(
                self.new.build_stats_tube(name, now),
                self.old.build_stats_tube(name, now),
                "stats-tube {name}"
            );
        }
        for id in 0..MAX_JOB + 48 {
            assert_eq!(
                self.new.build_stats_job(id, now),
                self.old.build_stats_job(id, now),
                "stats-job {id}"
            );
        }
        for c in 0..CONNS {
            assert_eq!(
                self.new.watched_tube_names(c),
                self.old.watched_tube_names(c),
                "watch list of {c}"
            );
        }
    }
}

fn run_steps(steps: Vec<(Msg, bool)>, journal: bool) {
    let mut p = Pair::new(journal);
    p.compare();
    for (msg, tick_after) in steps {
        p.run(msg, tick_after);
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 10_000, ..ProptestConfig::default() })]

    #[test]
    fn new_engine_matches_frozen_oracle(steps in prop::collection::vec(step(), 20..100)) {
        run_steps(steps, false);
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 4_000, ..ProptestConfig::default() })]

    #[test]
    fn journaling_engine_matches_frozen_oracle(steps in prop::collection::vec(step(), 20..100)) {
        run_steps(steps, true);
    }
}
