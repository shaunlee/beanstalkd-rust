//! Journaling (`take_journal`, `set_binlog_stats`) and recovery
//! (`Engine::recover`) tests. Ground truth: docs/PLAN.md §4.1, prot.c
//! (`enqueue_job` / `bury_job` `update_store` arguments, `prot_replay`) and
//! file.c (`readrec`), confirmed against the reference run with `-b`.
//!
//! A restart is simulated by feeding the journal to `replay`, a minimal
//! model of the store: the last record of a job wins, jobs are listed in
//! order of their first record, and the next id is the highest id in any
//! record + 1.

#![allow(clippy::unwrap_used)]

use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;
use proptest::prelude::*;

use bstk_proto::{Command, JobId, PutRejection, Response, StatsJob, StatsTube, TubeName};

use crate::model::{JobRec, JobState};
use crate::oracle_tests::{MAX_JOB, Pair, step};
use crate::{
    BinlogStats, ConnId, Engine, EngineConfig, JobRecord, JournalEntry, NANOS_PER_SEC, Nanos,
    Outbox, RecordState, RecoveredJob, Recovery, StaticSysInfo,
};

const SEC: Nanos = NANOS_PER_SEC;

fn cfg(journal: bool) -> EngineConfig {
    EngineConfig {
        journal,
        ..EngineConfig::default()
    }
}

fn engine(now: Nanos, journal: bool) -> Engine {
    Engine::new(now, cfg(journal), Box::new(StaticSysInfo::default()))
}

fn recover(now: Nanos, journal: bool, recovery: Recovery) -> Engine {
    Engine::recover(
        now,
        cfg(journal),
        Box::new(StaticSysInfo::default()),
        recovery,
    )
}

fn tube(name: &str) -> TubeName {
    TubeName::new(name).unwrap()
}

fn cmd(e: &mut Engine, now: Nanos, c: ConnId, command: Command) -> Outbox {
    let mut out = Outbox::new();
    e.handle(now, c, command, &mut out);
    out
}

fn put(e: &mut Engine, now: Nanos, c: ConnId, pri: u32, delay: u32, ttr: u32, body: &str) -> JobId {
    let out = cmd(
        e,
        now,
        c,
        Command::Put {
            pri,
            delay,
            ttr,
            body: Bytes::copy_from_slice(body.as_bytes()),
        },
    );
    match out.last() {
        Some((_, Response::Inserted(id))) => *id,
        other => panic!("expected INSERTED, got {other:?}"),
    }
}

fn journal(e: &mut Engine) -> Vec<JournalEntry> {
    let mut buf = Vec::new();
    e.take_journal(&mut buf);
    buf
}

fn stats_job(e: &Engine, now: Nanos, id: JobId) -> StatsJob {
    e.build_stats_job(id, now).unwrap()
}

fn stats_tube(e: &Engine, now: Nanos, name: &str) -> StatsTube {
    e.build_stats_tube(&tube(name), now).unwrap()
}

fn names(e: &Engine) -> Vec<String> {
    e.tube_names().iter().map(|t| t.to_string()).collect()
}

/// Maps a live job to the record the reference writes for it. Independent
/// of the engine's own journaling code.
fn raw_record(j: &JobRec) -> JobRecord {
    let (state, deadline_at) = match j.state {
        JobState::Ready | JobState::Reserved => (RecordState::Ready, 0),
        JobState::Delayed => (RecordState::Delayed, j.deadline_at),
        JobState::Buried => (RecordState::Buried, 0),
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

fn update_of(entries: &[JournalEntry]) -> Vec<JobRecord> {
    entries
        .iter()
        .map(|e| match e {
            JournalEntry::Update(r) => r.clone(),
            other => panic!("expected Update, got {other:?}"),
        })
        .collect()
}

/// In-test store model: last record wins, first-record order, next id =
/// highest id seen in any record + 1, and the reference's replay tube list
/// (append on a job's full record, swap-remove when a delete frees a tube's
/// last job).
fn replay(entries: &[JournalEntry]) -> Recovery {
    let mut order: Vec<JobId> = Vec::new();
    let mut live: HashMap<JobId, RecoveredJob> = HashMap::new();
    let mut max_id: JobId = 0;
    let mut tube_order: Vec<TubeName> = Vec::new();
    let mut tube_refs: HashMap<TubeName, u32> = HashMap::new();
    for entry in entries {
        match entry {
            JournalEntry::Put { record, tube, body } => {
                max_id = max_id.max(record.id);
                order.push(record.id);
                if tube.as_str() != "default" {
                    let refs = tube_refs.entry(tube.clone()).or_insert(0);
                    if *refs == 0 {
                        tube_order.push(tube.clone());
                    }
                    *refs += 1;
                }
                live.insert(
                    record.id,
                    RecoveredJob {
                        record: record.clone(),
                        tube: tube.clone(),
                        body: body.clone(),
                    },
                );
            }
            JournalEntry::Update(record) => {
                max_id = max_id.max(record.id);
                if let Some(j) = live.get_mut(&record.id) {
                    j.record = record.clone();
                }
            }
            JournalEntry::Delete(id) => {
                max_id = max_id.max(*id);
                if let Some(j) = live.remove(id)
                    && let Some(refs) = tube_refs.get_mut(&j.tube)
                {
                    *refs -= 1;
                    if *refs == 0
                        && let Some(pos) = tube_order.iter().position(|t| *t == j.tube)
                    {
                        tube_order.swap_remove(pos);
                    }
                }
            }
        }
    }
    Recovery {
        jobs: order.iter().filter_map(|id| live.remove(id)).collect(),
        next_id: max_id + 1,
        tube_order,
    }
}

fn rec(id: JobId, state: RecordState, deadline_at: Nanos) -> JobRecord {
    JobRecord {
        id,
        pri: 100,
        delay: 0,
        ttr: 60,
        created_at: 0,
        deadline_at,
        state,
        reserve_ct: 0,
        timeout_ct: 0,
        release_ct: 0,
        bury_ct: 0,
        kick_ct: 0,
    }
}

fn rjob(record: JobRecord, t: &str) -> RecoveredJob {
    RecoveredJob {
        body: Bytes::from(format!("body{}", record.id)),
        record,
        tube: tube(t),
    }
}

/// Buried order of `name`: repeatedly peek-buried and kick one.
fn buried_order(e: &mut Engine, now: Nanos, c: ConnId, name: &TubeName) -> Vec<JobId> {
    cmd(e, now, c, Command::Use(name.clone()));
    let mut ids = Vec::new();
    loop {
        match cmd(e, now, c, Command::PeekBuried).pop() {
            Some((_, Response::Found { id, .. })) => ids.push(id),
            _ => break,
        }
        cmd(e, now, c, Command::Kick(1));
    }
    ids
}

/// Ready order across `tubes`: reserve-with-timeout 0 until nothing is left.
fn ready_order(e: &mut Engine, now: Nanos, c: ConnId, tubes: &[TubeName]) -> Vec<JobId> {
    for t in tubes {
        cmd(e, now, c, Command::Watch(t.clone()));
    }
    let mut ids = Vec::new();
    while let Some((_, Response::Reserved { id, .. })) =
        cmd(e, now, c, Command::ReserveWithTimeout(0)).pop()
    {
        ids.push(id);
    }
    ids
}

// ---------------------------------------------------------------------
// Journaled transitions
// ---------------------------------------------------------------------

#[test]
fn put_journals_full_record() {
    let t0 = 7 * SEC;
    let mut e = engine(t0, true);
    e.connect(t0, 1);
    cmd(&mut e, t0, 1, Command::Use(tube("t")));
    let a = put(&mut e, t0, 1, 5, 0, 0, "a");
    let b = put(&mut e, t0 + 3, 1, 2000, 4, 9, "bb");
    let j = journal(&mut e);
    let mut ra = rec(a, RecordState::Ready, 0);
    ra.pri = 5;
    ra.ttr = 1; // TTR 0 is bumped to 1
    ra.created_at = t0;
    let mut rb = rec(b, RecordState::Delayed, t0 + 3 + 4 * SEC);
    rb.pri = 2000;
    rb.delay = 4;
    rb.ttr = 9;
    rb.created_at = t0 + 3;
    assert_eq!(
        j,
        vec![
            JournalEntry::Put {
                record: ra,
                tube: tube("t"),
                body: Bytes::from_static(b"a"),
            },
            JournalEntry::Put {
                record: rb,
                tube: tube("t"),
                body: Bytes::from_static(b"bb"),
            },
        ]
    );
    // Draining leaves the buffer empty.
    assert!(journal(&mut e).is_empty());
}

#[test]
fn put_to_waiting_reserver_journals_ready_record() {
    // enqueue_job writes before process_queue hands the job out.
    let mut e = engine(0, true);
    e.connect(0, 1);
    e.connect(0, 2);
    assert!(cmd(&mut e, 0, 2, Command::Reserve).is_empty());
    let id = put(&mut e, 0, 1, 1, 0, 10, "x");
    assert_eq!(stats_job(&e, 0, id).state, "reserved");
    let j = journal(&mut e);
    match &j[..] {
        [JournalEntry::Put { record, .. }] => {
            assert_eq!(record.state, RecordState::Ready);
            assert_eq!(record.reserve_ct, 0);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn put_started_then_put_journals_header_time_created_at() {
    let mut e = engine(0, true);
    e.connect(0, 1);
    e.put_started(5, 1, false);
    let id = put(&mut e, 9, 1, 1, 0, 10, "x");
    match &journal(&mut e)[..] {
        [JournalEntry::Put { record, .. }] => {
            assert_eq!((record.id, record.created_at), (id, 5));
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn release_journals_only_with_delay() {
    let mut e = engine(0, true);
    e.connect(0, 1);
    let id = put(&mut e, 0, 1, 1, 0, 10, "x");
    cmd(&mut e, 0, 1, Command::Reserve);
    cmd(&mut e, 0, 1, Command::Touch(id));
    let out = cmd(
        &mut e,
        SEC,
        1,
        Command::Release {
            id,
            pri: 7,
            delay: 0,
        },
    );
    assert_eq!(out, vec![(1, Response::Released)]);
    journal(&mut e); // the put
    assert!(journal(&mut e).is_empty());

    cmd(&mut e, SEC, 1, Command::ReserveJob(id));
    let out = cmd(
        &mut e,
        2 * SEC,
        1,
        Command::Release {
            id,
            pri: 8,
            delay: 3,
        },
    );
    assert_eq!(out, vec![(1, Response::Released)]);
    let j = update_of(&journal(&mut e));
    let mut want = rec(id, RecordState::Delayed, 5 * SEC);
    want.pri = 8;
    want.delay = 3;
    want.ttr = 10;
    want.reserve_ct = 2;
    want.release_ct = 2;
    assert_eq!(j, vec![want]);
}

#[test]
fn bury_journals_buried_record() {
    let mut e = engine(0, true);
    e.connect(0, 1);
    let id = put(&mut e, 0, 1, 1, 0, 10, "x");
    cmd(&mut e, 0, 1, Command::Reserve);
    journal(&mut e);
    assert_eq!(
        cmd(&mut e, SEC, 1, Command::Bury { id, pri: 42 }),
        vec![(1, Response::Buried)]
    );
    let j = update_of(&journal(&mut e));
    let mut want = rec(id, RecordState::Buried, 0);
    want.pri = 42;
    want.ttr = 10;
    want.reserve_ct = 1;
    want.bury_ct = 1;
    assert_eq!(j, vec![want]);
    // Failed bury / release: nothing.
    cmd(&mut e, SEC, 1, Command::Bury { id, pri: 1 });
    cmd(
        &mut e,
        SEC,
        1,
        Command::Release {
            id,
            pri: 1,
            delay: 5,
        },
    );
    assert!(journal(&mut e).is_empty());
}

#[test]
fn kick_journals_each_kicked_job_after_the_transition() {
    let mut e = engine(0, true);
    e.connect(0, 1);
    let ids: Vec<JobId> = (0..3).map(|_| put(&mut e, 0, 1, 1, 0, 10, "x")).collect();
    // Bury in order 2, 0, 1 so the FIFO differs from id order.
    for &i in &[ids[2], ids[0], ids[1]] {
        cmd(&mut e, 0, 1, Command::ReserveJob(i));
        cmd(&mut e, 0, 1, Command::Bury { id: i, pri: 3 });
    }
    journal(&mut e);
    assert_eq!(
        cmd(&mut e, 0, 1, Command::Kick(2)),
        vec![(1, Response::Kicked(2))]
    );
    let j = update_of(&journal(&mut e));
    assert_eq!(
        j.iter().map(|r| r.id).collect::<Vec<_>>(),
        vec![ids[2], ids[0]]
    );
    for r in &j {
        assert_eq!(r.state, RecordState::Ready);
        assert_eq!((r.bury_ct, r.kick_ct, r.reserve_ct), (1, 1, 1));
        assert_eq!(r.deadline_at, 0);
    }
    // Kicking with nothing to kick journals nothing.
    cmd(&mut e, 0, 1, Command::Kick(5));
    journal(&mut e);
    assert_eq!(
        cmd(&mut e, 0, 1, Command::Kick(5)),
        vec![(1, Response::Kicked(0))]
    );
    assert!(journal(&mut e).is_empty());
}

#[test]
fn kick_delayed_journals_ready_record() {
    let mut e = engine(0, true);
    e.connect(0, 1);
    let id = put(&mut e, 0, 1, 1, 50, 10, "x");
    journal(&mut e);
    cmd(&mut e, SEC, 1, Command::Kick(1));
    let j = update_of(&journal(&mut e));
    assert_eq!(j.len(), 1);
    assert_eq!(
        (
            j[0].id,
            j[0].state,
            j[0].deadline_at,
            j[0].delay,
            j[0].kick_ct
        ),
        (id, RecordState::Ready, 0, 50, 1)
    );
}

#[test]
fn kick_job_journals_before_handing_to_waiter() {
    let mut e = engine(0, true);
    e.connect(0, 1);
    e.connect(0, 2);
    let a = put(&mut e, 0, 1, 1, 50, 10, "x");
    let b = put(&mut e, 0, 1, 1, 0, 10, "y");
    cmd(&mut e, 0, 1, Command::ReserveJob(b));
    cmd(&mut e, 0, 1, Command::Bury { id: b, pri: 1 });
    cmd(&mut e, 0, 2, Command::Reserve); // waits
    journal(&mut e);
    let out = cmd(&mut e, 0, 1, Command::KickJob(a));
    assert!(out.contains(&(1, Response::KickedJob)));
    assert_eq!(stats_job(&e, 0, a).state, "reserved");
    let out = cmd(&mut e, 0, 1, Command::KickJob(b));
    assert_eq!(out, vec![(1, Response::KickedJob)]);
    let j = update_of(&journal(&mut e));
    assert_eq!(j.len(), 2);
    assert_eq!(
        (j[0].id, j[0].state, j[0].reserve_ct, j[0].kick_ct),
        (a, RecordState::Ready, 0, 1)
    );
    assert_eq!(
        (j[1].id, j[1].state, j[1].bury_ct, j[1].kick_ct),
        (b, RecordState::Ready, 1, 1)
    );
    // kick-job of a ready / reserved / missing job: nothing.
    cmd(&mut e, 0, 1, Command::KickJob(a));
    cmd(&mut e, 0, 1, Command::KickJob(b));
    cmd(&mut e, 0, 1, Command::KickJob(99));
    assert!(journal(&mut e).is_empty());
}

#[test]
fn delete_journals_in_every_state() {
    let mut e = engine(0, true);
    e.connect(0, 1);
    let ready = put(&mut e, 0, 1, 1, 0, 10, "r");
    let delayed = put(&mut e, 0, 1, 1, 9, 10, "d");
    let buried = put(&mut e, 0, 1, 1, 0, 10, "b");
    let reserved = put(&mut e, 0, 1, 1, 0, 10, "v");
    cmd(&mut e, 0, 1, Command::ReserveJob(buried));
    cmd(&mut e, 0, 1, Command::Bury { id: buried, pri: 1 });
    cmd(&mut e, 0, 1, Command::ReserveJob(reserved));
    journal(&mut e);
    for id in [ready, delayed, buried, reserved] {
        assert_eq!(
            cmd(&mut e, 0, 1, Command::Delete(id)),
            vec![(1, Response::Deleted)]
        );
    }
    assert_eq!(
        journal(&mut e),
        vec![
            JournalEntry::Delete(ready),
            JournalEntry::Delete(delayed),
            JournalEntry::Delete(buried),
            JournalEntry::Delete(reserved),
        ]
    );
    // Not found, or reserved by another connection: nothing.
    let other = put(&mut e, 0, 1, 1, 0, 10, "o");
    e.connect(0, 2);
    cmd(&mut e, 0, 2, Command::ReserveJob(other));
    journal(&mut e);
    cmd(&mut e, 0, 1, Command::Delete(other));
    cmd(&mut e, 0, 1, Command::Delete(999));
    assert!(journal(&mut e).is_empty());
}

#[test]
fn non_journaled_transitions_produce_nothing() {
    let mut e = engine(0, true);
    e.connect(0, 1);
    e.connect(0, 2);
    let a = put(&mut e, 0, 1, 1, 0, 1, "a");
    let b = put(&mut e, 0, 1, 1, 2, 10, "b");
    let c = put(&mut e, 0, 1, 1, 0, 10, "c");
    journal(&mut e);

    let mut out = Outbox::new();
    // reserve, reserve-job, touch, peeks, stats, use/watch/ignore, pause.
    cmd(&mut e, 0, 1, Command::Reserve);
    cmd(&mut e, 0, 1, Command::Touch(a));
    cmd(&mut e, 0, 2, Command::ReserveJob(c));
    cmd(&mut e, 0, 1, Command::Peek(a));
    cmd(&mut e, 0, 1, Command::PeekReady);
    cmd(&mut e, 0, 1, Command::PeekDelayed);
    cmd(&mut e, 0, 1, Command::PeekBuried);
    cmd(&mut e, 0, 1, Command::StatsJob(a));
    cmd(&mut e, 0, 1, Command::StatsTube(tube("default")));
    cmd(&mut e, 0, 1, Command::Stats);
    cmd(&mut e, 0, 1, Command::ListTubes);
    cmd(&mut e, 0, 1, Command::Use(tube("x")));
    cmd(&mut e, 0, 1, Command::Watch(tube("x")));
    cmd(&mut e, 0, 1, Command::Ignore(tube("x")));
    cmd(&mut e, 0, 1, Command::Use(tube("default")));
    cmd(
        &mut e,
        0,
        1,
        Command::PauseTube {
            tube: tube("default"),
            delay: 0,
        },
    );
    // TTR timeout of `a` and delay expiry of `b`.
    e.tick(3 * SEC, &mut out);
    assert_eq!(stats_job(&e, 3 * SEC, a).timeouts, 1);
    assert_eq!(stats_job(&e, 3 * SEC, b).state, "ready");
    // Disconnect releasing `c`.
    e.disconnect(3 * SEC, 2, &mut out);
    assert_eq!(stats_job(&e, 3 * SEC, c).state, "ready");
    assert!(journal(&mut e).is_empty());

    // Rejected and draining puts, and a put whose body never arrives.
    e.put_rejected(3 * SEC, 1, PutRejection::JobTooBig, &mut out);
    e.put_rejected(3 * SEC, 1, PutRejection::ExpectedCrlf, &mut out);
    e.put_rejected(3 * SEC, 1, PutRejection::TrailingGarbage, &mut out);
    e.put_rejected(3 * SEC, 1, PutRejection::OutOfMemory, &mut out);
    e.put_started(3 * SEC, 1, false);
    e.put_rejected(3 * SEC, 1, PutRejection::ExpectedCrlf, &mut out);
    e.connect(3 * SEC, 3);
    e.put_started(3 * SEC, 3, false);
    e.disconnect(3 * SEC, 3, &mut out);
    e.set_draining(true);
    let out = cmd(
        &mut e,
        3 * SEC,
        1,
        Command::Put {
            pri: 1,
            delay: 0,
            ttr: 1,
            body: Bytes::new(),
        },
    );
    assert_eq!(out, vec![(1, Response::Draining)]);
    assert!(journal(&mut e).is_empty());
}

#[test]
fn journal_off_records_nothing() {
    let mut e = engine(0, false);
    e.connect(0, 1);
    let a = put(&mut e, 0, 1, 1, 0, 10, "a");
    let b = put(&mut e, 0, 1, 1, 5, 10, "b");
    cmd(&mut e, 0, 1, Command::ReserveJob(a));
    cmd(
        &mut e,
        0,
        1,
        Command::Release {
            id: a,
            pri: 1,
            delay: 5,
        },
    );
    cmd(&mut e, 0, 1, Command::Kick(5));
    cmd(&mut e, 0, 1, Command::ReserveJob(a));
    cmd(&mut e, 0, 1, Command::Bury { id: a, pri: 1 });
    cmd(&mut e, 0, 1, Command::KickJob(a));
    cmd(&mut e, 0, 1, Command::Delete(b));
    let mut buf = Vec::new();
    e.take_journal(&mut buf);
    assert!(buf.is_empty());
    assert_eq!(e.t_journal_capacity(), 0, "journal off must not allocate");
    assert_eq!(stats_job(&e, 0, a).file, 0);
}

#[test]
fn take_journal_appends_in_order() {
    let mut e = engine(0, true);
    e.connect(0, 1);
    let a = put(&mut e, 0, 1, 1, 0, 10, "a");
    let mut buf = vec![JournalEntry::Delete(77)];
    e.take_journal(&mut buf);
    let b = put(&mut e, 0, 1, 1, 0, 10, "b");
    cmd(&mut e, 0, 1, Command::Delete(a));
    e.take_journal(&mut buf);
    let ids: Vec<String> = buf
        .iter()
        .map(|en| match en {
            JournalEntry::Put { record, .. } => format!("put{}", record.id),
            JournalEntry::Update(r) => format!("upd{}", r.id),
            JournalEntry::Delete(id) => format!("del{id}"),
        })
        .collect();
    assert_eq!(
        ids,
        vec![
            "del77".to_string(),
            format!("put{a}"),
            format!("put{b}"),
            format!("del{a}")
        ]
    );
}

#[test]
fn binlog_stats_and_file() {
    let mut e = engine(0, true);
    e.connect(0, 1);
    let s = e.build_stats_server(0);
    assert_eq!(
        (
            s.binlog_oldest_index,
            s.binlog_current_index,
            s.binlog_records_written,
            s.binlog_records_migrated
        ),
        (0, 0, 0, 0)
    );
    let a = put(&mut e, 0, 1, 1, 0, 10, "a");
    e.set_binlog_stats(BinlogStats {
        oldest_index: 2,
        current_index: 5,
        records_written: 11,
        records_migrated: 3,
    });
    let b = put(&mut e, 0, 1, 1, 0, 10, "b");
    let s = e.build_stats_server(0);
    assert_eq!(
        (
            s.binlog_oldest_index,
            s.binlog_current_index,
            s.binlog_records_written,
            s.binlog_records_migrated
        ),
        (2, 5, 11, 3)
    );
    assert_eq!(stats_job(&e, 0, a).file, 0);
    assert_eq!(stats_job(&e, 0, b).file, 5);
    // Later transitions don't move the job's file.
    e.set_binlog_stats(BinlogStats {
        current_index: 6,
        ..BinlogStats::default()
    });
    cmd(&mut e, 0, 1, Command::ReserveJob(b));
    cmd(&mut e, 0, 1, Command::Bury { id: b, pri: 1 });
    assert_eq!(stats_job(&e, 0, b).file, 5);

    // With journaling off the stats are still reported but `file` stays 0.
    let mut e = engine(0, false);
    e.connect(0, 1);
    e.set_binlog_stats(BinlogStats {
        current_index: 4,
        ..BinlogStats::default()
    });
    let c = put(&mut e, 0, 1, 1, 0, 10, "c");
    assert_eq!(stats_job(&e, 0, c).file, 0);
    assert_eq!(e.build_stats_server(0).binlog_current_index, 4);
}

// ---------------------------------------------------------------------
// Recovery rules
// ---------------------------------------------------------------------

#[test]
fn recover_creates_tubes_in_first_appearance_order() {
    let recovery = Recovery {
        jobs: vec![
            rjob(rec(4, RecordState::Ready, 0), "b"),
            rjob(rec(2, RecordState::Buried, 0), "a"),
            rjob(rec(9, RecordState::Ready, 0), "b"),
            rjob(rec(3, RecordState::Ready, 0), "default"),
            rjob(rec(5, RecordState::Delayed, 99 * SEC), "c"),
        ],
        next_id: 10,
        tube_order: Vec::new(),
    };
    let e = recover(SEC, false, recovery);
    assert_eq!(names(&e), vec!["default", "b", "a", "c"]);
    assert_eq!(e.build_stats_server(SEC).current_tubes, 4);
    // No jobs at all: only "default".
    let e = recover(
        SEC,
        false,
        Recovery {
            jobs: vec![],
            next_id: 1,
            tube_order: Vec::new(),
        },
    );
    assert_eq!(names(&e), vec!["default"]);
    e.t_check_indexes();
}

#[test]
fn recover_buried_in_replay_order_with_extra_bury() {
    let mut r1 = rec(7, RecordState::Buried, 0);
    r1.bury_ct = 1;
    r1.reserve_ct = 1;
    let mut r2 = rec(3, RecordState::Buried, 0);
    r2.bury_ct = 4;
    r2.kick_ct = 3;
    let mut e = recover(
        SEC,
        true,
        Recovery {
            jobs: vec![rjob(r1, "t"), rjob(r2, "t")],
            next_id: 8,
            tube_order: Vec::new(),
        },
    );
    let s = stats_job(&e, SEC, 7);
    assert_eq!((s.state, s.buries, s.reserves), ("buried", 2, 1));
    let s = stats_job(&e, SEC, 3);
    assert_eq!((s.state, s.buries, s.kicks), ("buried", 5, 3));
    let t = stats_tube(&e, SEC, "t");
    assert_eq!(t.current_jobs_buried, 2);
    assert_eq!(e.build_stats_server(SEC).current_jobs_buried, 2);
    e.connect(SEC, 1);
    assert_eq!(buried_order(&mut e, SEC, 1, &tube("t")), vec![7, 3]);
    // Kicks after recovery are journaled as updates of the old jobs.
    let j = update_of(&journal(&mut e));
    assert_eq!(
        j.iter()
            .map(|r| (r.id, r.bury_ct, r.kick_ct))
            .collect::<Vec<_>>(),
        vec![(7, 2, 1), (3, 5, 4)]
    );
}

#[test]
fn recover_delayed_by_deadline() {
    let now = 100 * SEC;
    let mut past = rec(1, RecordState::Delayed, now - 1);
    past.delay = 30;
    let mut exact = rec(2, RecordState::Delayed, now);
    exact.delay = 5;
    let mut future = rec(3, RecordState::Delayed, now + 1);
    future.delay = 7;
    let mut later = rec(4, RecordState::Delayed, now + 20 * SEC + 5);
    later.delay = 60;
    let e_jobs = vec![
        rjob(past, "a"),
        rjob(exact, "a"),
        rjob(future, "a"),
        rjob(later, "b"),
    ];
    let mut e = recover(
        now,
        false,
        Recovery {
            jobs: e_jobs,
            next_id: 5,
            tube_order: Vec::new(),
        },
    );
    let s = stats_job(&e, now, 1);
    assert_eq!((s.state, s.delay, s.time_left), ("ready", 30, 0));
    let s = stats_job(&e, now, 2);
    assert_eq!((s.state, s.delay), ("ready", 5));
    let s = stats_job(&e, now, 3);
    assert_eq!((s.state, s.delay), ("delayed", 7));
    let s = stats_job(&e, now, 4);
    assert_eq!((s.state, s.delay, s.time_left), ("delayed", 60, 20));
    assert_eq!(e.next_deadline(), Some(now + 1));
    assert_eq!(e.build_stats_server(now).current_jobs_delayed, 2);
    assert_eq!(e.build_stats_server(now).current_jobs_ready, 2);
    e.t_check_indexes();
    // The original deadlines hold.
    let mut out = Outbox::new();
    e.tick(now + 1, &mut out);
    assert_eq!(stats_job(&e, now + 1, 3).state, "ready");
    assert_eq!(e.next_deadline(), Some(now + 20 * SEC + 5));
    e.tick(now + 20 * SEC + 4, &mut out);
    assert_eq!(stats_job(&e, now, 4).state, "delayed");
    e.tick(now + 20 * SEC + 5, &mut out);
    assert_eq!(stats_job(&e, now, 4).state, "ready");
}

#[test]
fn recover_ready_counts_and_order() {
    let mut a = rec(5, RecordState::Ready, 0);
    a.pri = 1023;
    let mut b = rec(2, RecordState::Ready, 0);
    b.pri = 1024;
    let mut c = rec(9, RecordState::Ready, 0);
    c.pri = 0;
    let mut d = rec(1, RecordState::Buried, 0);
    d.pri = 0; // urgent counts only ready jobs
    let mut e = recover(
        SEC,
        false,
        Recovery {
            jobs: vec![rjob(a, "x"), rjob(b, "y"), rjob(c, "x"), rjob(d, "x")],
            next_id: 10,
            tube_order: Vec::new(),
        },
    );
    let s = e.build_stats_server(SEC);
    assert_eq!(
        (
            s.current_jobs_urgent,
            s.current_jobs_ready,
            s.current_jobs_buried
        ),
        (2, 3, 1)
    );
    let t = stats_tube(&e, SEC, "x");
    assert_eq!(
        (
            t.current_jobs_urgent,
            t.current_jobs_ready,
            t.current_jobs_buried
        ),
        (2, 2, 1)
    );
    let t = stats_tube(&e, SEC, "y");
    assert_eq!((t.current_jobs_urgent, t.current_jobs_ready), (0, 1));
    e.connect(SEC, 1);
    assert_eq!(
        ready_order(&mut e, SEC, 1, &[tube("x"), tube("y")]),
        vec![9, 5, 2]
    );
    crate::engine::proptests::check_invariants(&e);
}

#[test]
fn recover_next_id_and_counters() {
    let t0 = 1000 * SEC;
    let mut r = rec(4, RecordState::Ready, 0);
    r.created_at = t0 - 90 * SEC;
    r.reserve_ct = 3;
    r.timeout_ct = 2;
    r.release_ct = 1;
    let mut e = recover(
        t0,
        true,
        Recovery {
            jobs: vec![rjob(r, "q")],
            next_id: 12,
            tube_order: Vec::new(),
        },
    );
    let s = e.build_stats_server(t0);
    assert_eq!(
        (
            s.total_jobs,
            s.cmd_put,
            s.cmd_delete,
            s.uptime,
            s.job_timeouts
        ),
        (0, 0, 0, 0, 0)
    );
    assert_eq!((s.current_connections, s.total_connections), (0, 0));
    let t = stats_tube(&e, t0, "q");
    assert_eq!(
        (
            t.total_jobs,
            t.cmd_delete,
            t.current_using,
            t.current_watching
        ),
        (0, 0, 0, 0)
    );
    let j = stats_job(&e, t0 + 5 * SEC, 4);
    assert_eq!(
        (j.age, j.reserves, j.timeouts, j.releases, j.file),
        (95, 3, 2, 1, 0)
    );
    assert_eq!(e.build_stats_server(t0 + 5 * SEC).uptime, 5);
    assert!(journal(&mut e).is_empty(), "recovery must not journal");

    e.connect(t0, 1);
    assert_eq!(put(&mut e, t0, 1, 1, 0, 1, "n"), 12);
    // A deleted recovered job journals a Delete; the tube goes away.
    cmd(&mut e, t0, 1, Command::Delete(4));
    let j = journal(&mut e);
    assert!(matches!(j[0], JournalEntry::Put { .. }));
    assert_eq!(j[1], JournalEntry::Delete(4));
    assert!(!e.t_tube_exists(&tube("q")));
    assert_eq!(stats_tube(&e, t0, "default").cmd_delete, 0);

    // next_id is authoritative, but never reuses a live id or goes below 1.
    let e = recover(
        t0,
        false,
        Recovery {
            jobs: vec![rjob(rec(8, RecordState::Ready, 0), "q")],
            next_id: 3,
            tube_order: Vec::new(),
        },
    );
    assert_eq!(e.t_next_job_id(), 9);
    let e = recover(
        t0,
        false,
        Recovery {
            jobs: vec![],
            next_id: 0,
            tube_order: Vec::new(),
        },
    );
    assert_eq!(e.t_next_job_id(), 1);
}

#[test]
fn journal_replay_round_trip_matches_reference_restart() {
    // Mirrors a scenario run against the reference with `-b`, killed with
    // SIGKILL and restarted on the same directory. Expected values are the
    // reference's, except `list-tubes` (see below).
    let t0 = 1_000 * SEC;
    let mut e = engine(t0, true);
    e.connect(t0, 1);
    let c = 1;
    let use_t = |e: &mut Engine, t: &str| {
        cmd(e, t0, c, Command::Use(tube(t)));
        cmd(e, t0, c, Command::Watch(tube(t)));
    };
    assert_eq!(put(&mut e, t0, c, 50, 0, 60, "j1"), 1);
    use_t(&mut e, "t1");
    put(&mut e, t0, c, 10, 0, 60, "j2");
    put(&mut e, t0, c, 100, 100, 60, "j3");
    use_t(&mut e, "t2");
    put(&mut e, t0, c, 2000, 0, 60, "j4");
    put(&mut e, t0, c, 100, 1, 60, "j5");
    use_t(&mut e, "t3");
    put(&mut e, t0, c, 100, 0, 60, "j6");
    use_t(&mut e, "t4");
    put(&mut e, t0, c, 5, 0, 60, "j7");
    use_t(&mut e, "t5");
    put(&mut e, t0, c, 100, 0, 60, "j8");
    use_t(&mut e, "t1");
    assert_eq!(put(&mut e, t0, c, 10, 0, 60, "j9"), 9);
    cmd(&mut e, t0, c, Command::Delete(6));
    for (id, pri) in [(9, 20), (2, 30)] {
        cmd(&mut e, t0, c, Command::ReserveJob(id));
        cmd(&mut e, t0, c, Command::Bury { id, pri });
    }
    cmd(&mut e, t0, c, Command::ReserveJob(7));
    cmd(&mut e, t0, c, Command::Bury { id: 7, pri: 5 });
    cmd(&mut e, t0, c, Command::Use(tube("t4")));
    cmd(&mut e, t0, c, Command::Kick(1));
    cmd(&mut e, t0, c, Command::ReserveJob(1));
    cmd(
        &mut e,
        t0,
        c,
        Command::Release {
            id: 1,
            pri: 51,
            delay: 200,
        },
    );
    cmd(&mut e, t0, c, Command::ReserveJob(8));
    cmd(
        &mut e,
        t0,
        c,
        Command::Release {
            id: 8,
            pri: 99,
            delay: 0,
        },
    );
    cmd(&mut e, t0, c, Command::ReserveJob(8));
    cmd(&mut e, t0, c, Command::ReserveJob(4));
    assert_eq!(put(&mut e, t0, c, 1, 0, 60, "j10"), 10);
    cmd(&mut e, t0, c, Command::Delete(10));
    let entries = journal(&mut e);
    // The reference reported binlog-records-written = 17.
    assert_eq!(entries.len(), 17);

    // Restart 1.5 s later.
    let t1 = t0 + 3 * SEC / 2;
    let recovery = replay(&entries);
    assert_eq!(recovery.next_id, 11);
    let mut r = recover(t1, true, recovery.clone());

    // Matches the reference: while replaying it creates t3 for job 6's put
    // record and destroys it at job 6's delete record (swap-remove moves t5
    // into t3's slot). The store reports that as `Recovery::tube_order`.
    assert_eq!(names(&r), vec!["default", "t1", "t2", "t5", "t4"]);

    let s = r.build_stats_server(t1);
    assert_eq!(
        (
            s.current_jobs_urgent,
            s.current_jobs_ready,
            s.current_jobs_reserved,
            s.current_jobs_delayed,
            s.current_jobs_buried,
            s.cmd_put,
            s.total_jobs,
            s.current_tubes,
        ),
        (3, 4, 0, 2, 2, 0, 0, 5)
    );
    let want_tubes = [
        ("default", (0, 0, 1, 0)),
        ("t1", (0, 0, 1, 2)),
        ("t2", (1, 2, 0, 0)),
        ("t4", (1, 1, 0, 0)),
        ("t5", (1, 1, 0, 0)),
    ];
    for (name, (urgent, ready, delayed, buried)) in want_tubes {
        let t = stats_tube(&r, t1, name);
        assert_eq!(
            (
                t.current_jobs_urgent,
                t.current_jobs_ready,
                t.current_jobs_reserved,
                t.current_jobs_delayed,
                t.current_jobs_buried,
                t.total_jobs,
                t.cmd_delete
            ),
            (urgent, ready, 0, delayed, buried, 0, 0),
            "{name}"
        );
    }
    // (state, pri, age, delay, time-left, reserves, releases, buries, kicks)
    type Row = (&'static str, u32, u64, u64, u64, u64, u64, u64, u64);
    let want_jobs: [(JobId, Row); 8] = [
        (1, ("delayed", 51, 1, 200, 198, 1, 1, 0, 0)),
        (2, ("buried", 30, 1, 0, 0, 1, 0, 2, 0)),
        (3, ("delayed", 100, 1, 100, 98, 0, 0, 0, 0)),
        (4, ("ready", 2000, 1, 0, 0, 0, 0, 0, 0)),
        (5, ("ready", 100, 1, 1, 0, 0, 0, 0, 0)),
        (7, ("ready", 5, 1, 0, 0, 1, 0, 1, 1)),
        (8, ("ready", 100, 1, 0, 0, 0, 0, 0, 0)),
        (9, ("buried", 20, 1, 0, 0, 1, 0, 2, 0)),
    ];
    for (id, want) in want_jobs {
        let j = stats_job(&r, t1, id);
        assert_eq!(
            (
                j.state,
                j.pri,
                j.age,
                j.delay,
                j.time_left,
                j.reserves,
                j.releases,
                j.buries,
                j.kicks
            ),
            want,
            "job {id}"
        );
    }
    for id in [6, 10, 11] {
        assert!(r.build_stats_job(id, t1).is_none());
    }
    assert!(journal(&mut r).is_empty());

    r.connect(t1, 1);
    assert_eq!(buried_order(&mut r, t1, 1, &tube("t1")), vec![2, 9]);
    let mut r = recover(t1, true, recovery.clone());
    r.connect(t1, 1);
    let all: Vec<TubeName> = ["t1", "t2", "t3", "t4", "t5"]
        .iter()
        .map(|t| tube(t))
        .collect();
    assert_eq!(ready_order(&mut r, t1, 1, &all), vec![7, 5, 8, 4]);
    assert_eq!(put(&mut r, t1, 1, 1, 0, 1, "new"), 11);
}

#[test]
fn reserved_at_crash_reverts_to_last_journaled_state() {
    // Reference-confirmed: a job reserved when the server dies comes back
    // in the state of its last record, with that record's counters.
    let t0 = 10 * SEC;
    let mut e = engine(t0, true);
    e.connect(t0, 1);
    let a = put(&mut e, t0, 1, 100, 0, 60, "a");
    cmd(&mut e, t0, 1, Command::ReserveJob(a));
    cmd(&mut e, t0, 1, Command::Bury { id: a, pri: 5 });
    cmd(&mut e, t0, 1, Command::ReserveJob(a));
    let b = put(&mut e, t0, 1, 100, 100, 60, "b");
    cmd(&mut e, t0, 1, Command::ReserveJob(b));
    let c = put(&mut e, t0, 1, 100, 1, 60, "c");
    cmd(&mut e, t0, 1, Command::ReserveJob(c));
    let d = put(&mut e, t0, 1, 100, 0, 1, "d");
    cmd(&mut e, t0, 1, Command::ReserveJob(d));
    let mut out = Outbox::new();
    e.tick(t0 + 2 * SEC + 1, &mut out); // d times out
    let f = put(&mut e, t0, 1, 100, 0, 60, "e");
    cmd(&mut e, t0, 1, Command::ReserveJob(f));
    cmd(
        &mut e,
        t0,
        1,
        Command::Release {
            id: f,
            pri: 7,
            delay: 0,
        },
    );
    let t1 = t0 + 3 * SEC;
    let r = recover(t1, false, replay(&journal(&mut e)));
    let got = |id| {
        let s = stats_job(&r, t1, id);
        (s.state, s.pri, s.reserves, s.timeouts, s.releases, s.buries)
    };
    assert_eq!(got(a), ("buried", 5, 1, 0, 0, 2));
    assert_eq!(got(b), ("delayed", 100, 0, 0, 0, 0));
    assert_eq!(got(c), ("ready", 100, 0, 0, 0, 0));
    assert_eq!(got(d), ("ready", 100, 0, 0, 0, 0));
    assert_eq!(got(f), ("ready", 100, 0, 0, 0, 0));
}

// ---------------------------------------------------------------------
// Proptest: random sequences, journal, restart, compare
// ---------------------------------------------------------------------

/// What the harness believes the binlog holds for one job, derived from the
/// live engine at each journaled transition (not from the engine's
/// journal).
#[derive(Debug, Clone)]
struct Persisted {
    seq: usize,
    record: JobRecord,
    tube: TubeName,
    body: Bytes,
}

type Snapshot = BTreeMap<JobId, (JobRec, TubeName)>;

fn snapshot(e: &Engine) -> Snapshot {
    e.t_all_job_ids()
        .into_iter()
        .filter_map(|id| e.t_job_raw(id).map(|j| (id, j)))
        .collect()
}

/// The record written by an `enqueue_job(..., 1)` that may have been
/// followed by `process_queue` reserving the job in the same call.
fn enqueued_record(j: &JobRec) -> JobRecord {
    let mut r = raw_record(j);
    if j.state == JobState::Reserved {
        r.reserve_ct -= 1;
    }
    r
}

fn entry_key(e: &JournalEntry) -> (JobId, u8) {
    match e {
        JournalEntry::Put { record, .. } => (record.id, 0),
        JournalEntry::Update(r) => (r.id, 1),
        JournalEntry::Delete(id) => (*id, 2),
    }
}

/// Journal entries implied by the change from `before` to `after` (one
/// engine call plus the tick after it), per docs/PLAN.md §4.1.
fn expected_entries(before: &Snapshot, after: &Snapshot) -> Vec<JournalEntry> {
    let mut want = Vec::new();
    for (&id, (b, _)) in before {
        let Some((a, _)) = after.get(&id) else {
            want.push(JournalEntry::Delete(id));
            continue;
        };
        let kicked = a.kick_ct > b.kick_ct;
        let buried = a.bury_ct > b.bury_ct;
        let released_with_delay = a.release_ct > b.release_ct && a.state == JobState::Delayed;
        if kicked || buried || released_with_delay {
            // A kicked job may have been handed to a waiter right after
            // its record was written.
            want.push(JournalEntry::Update(enqueued_record(a)));
        }
    }
    for (&id, (a, tube)) in after {
        if !before.contains_key(&id) {
            want.push(JournalEntry::Put {
                record: enqueued_record(a),
                tube: tube.clone(),
                body: a.body.clone(),
            });
        }
    }
    want.sort_by_key(entry_key);
    want
}

#[derive(Debug, Clone, Copy)]
enum Restart {
    After(Nanos),
    /// Exactly at the deadline of the n-th persisted delayed job, plus an
    /// offset in nanoseconds.
    AtDeadline(usize, i64),
}

fn restart() -> impl Strategy<Value = Restart> {
    prop_oneof![
        prop_oneof![Just(0), Just(1), Just(SEC), Just(3 * SEC), Just(10 * SEC)]
            .prop_map(Restart::After),
        (0..4usize, -1..=1i64).prop_map(|(n, off)| Restart::AtDeadline(n, off)),
    ]
}

fn run_restart(steps: Vec<(crate::oracle_tests::Msg, bool)>, restart: Restart) {
    let mut p = Pair::new(true);
    let mut persisted: HashMap<JobId, Persisted> = HashMap::new();
    let mut seq = 0usize;
    let mut max_put_id: JobId = 0;
    let mut journal_all: Vec<JournalEntry> = Vec::new();
    let mut before = snapshot(&p.new);
    for (msg, tick_after) in steps {
        p.run(msg, tick_after);
        let after = snapshot(&p.new);
        let want = expected_entries(&before, &after);
        let mut got = p.journal_buf.clone();
        // At most one put per call, so only update order is lost here;
        // `kick_journals_each_kicked_job_after_the_transition` covers it.
        got.sort_by_key(entry_key);
        assert_eq!(got, want, "journal at now={}", p.now);
        for entry in &want {
            match entry {
                JournalEntry::Put { record, tube, body } => {
                    max_put_id = max_put_id.max(record.id);
                    persisted.insert(
                        record.id,
                        Persisted {
                            seq,
                            record: record.clone(),
                            tube: tube.clone(),
                            body: body.clone(),
                        },
                    );
                    seq += 1;
                }
                JournalEntry::Update(r) => {
                    persisted.get_mut(&r.id).unwrap().record = r.clone();
                }
                JournalEntry::Delete(id) => {
                    persisted.remove(id);
                }
            }
        }
        journal_all.extend(p.journal_buf.iter().cloned());
        before = after;
    }

    // The persisted set is the live set, and a buried or delayed job's
    // record is its live state (only journaled transitions enter those
    // states or change them).
    let live = snapshot(&p.new);
    assert_eq!(live.keys().copied().collect::<Vec<_>>(), {
        let mut v: Vec<JobId> = persisted.keys().copied().collect();
        v.sort_unstable();
        v
    });
    for (id, (j, tube)) in &live {
        let pj = &persisted[id];
        assert_eq!(
            (&pj.tube, &pj.body, pj.record.created_at),
            (tube, &j.body, j.created_at)
        );
        if matches!(j.state, JobState::Buried | JobState::Delayed) {
            assert_eq!(pj.record, raw_record(j), "job {id}");
        }
    }

    let mut order: Vec<&Persisted> = persisted.values().collect();
    order.sort_by_key(|pj| pj.seq);
    let now2 = match restart {
        Restart::After(dt) => p.now + dt,
        Restart::AtDeadline(n, off) => {
            let deadlines: Vec<Nanos> = order
                .iter()
                .filter(|pj| pj.record.state == RecordState::Delayed)
                .map(|pj| pj.record.deadline_at)
                .collect();
            match deadlines.get(n) {
                Some(&d) => (d as i128 + off as i128).max(p.now as i128) as Nanos,
                None => p.now,
            }
        }
    };

    let recovery = replay(&journal_all);
    assert_eq!(recovery.next_id, max_put_id + 1);
    let mut r = recover(now2, true, recovery.clone());
    let mut buf = Vec::new();
    r.take_journal(&mut buf);
    assert!(buf.is_empty(), "recovery must not journal");
    r.t_check_indexes();
    crate::engine::proptests::check_invariants(&r);

    // Expected state, by the rules of docs/PLAN.md §4.1.
    let mut tubes: Vec<TubeName> = vec![TubeName::default_tube()];
    let mut want_jobs: HashMap<JobId, StatsJob> = HashMap::new();
    let mut tube_counts: HashMap<TubeName, [u64; 4]> = HashMap::new(); // urgent, ready, delayed, buried
    let mut buried: HashMap<TubeName, Vec<JobId>> = HashMap::new();
    let mut ready: Vec<(u32, JobId)> = Vec::new();
    let mut next_delay: Option<Nanos> = None;
    for pj in &order {
        let rd = &pj.record;
        if !tubes.contains(&pj.tube) {
            tubes.push(pj.tube.clone());
        }
        let counts = tube_counts.entry(pj.tube.clone()).or_default();
        let (state, time_left, buries) = match rd.state {
            RecordState::Buried => {
                counts[3] += 1;
                buried.entry(pj.tube.clone()).or_default().push(rd.id);
                ("buried", 0, rd.bury_ct + 1)
            }
            RecordState::Delayed if rd.deadline_at > now2 => {
                counts[2] += 1;
                next_delay = Some(next_delay.map_or(rd.deadline_at, |d| d.min(rd.deadline_at)));
                ("delayed", (rd.deadline_at - now2) / SEC, rd.bury_ct)
            }
            _ => {
                counts[1] += 1;
                if rd.pri < bstk_proto::URGENT_THRESHOLD {
                    counts[0] += 1;
                }
                ready.push((rd.pri, rd.id));
                ("ready", 0, rd.bury_ct)
            }
        };
        want_jobs.insert(
            rd.id,
            StatsJob {
                id: rd.id,
                tube: pj.tube.clone(),
                state,
                pri: rd.pri,
                age: now2.saturating_sub(rd.created_at) / SEC,
                delay: rd.delay as u64,
                ttr: rd.ttr as u64,
                time_left,
                file: 0,
                reserves: rd.reserve_ct as u64,
                timeouts: rd.timeout_ct as u64,
                releases: rd.release_ct as u64,
                buries: buries as u64,
                kicks: rd.kick_ct as u64,
            },
        );
    }

    assert_eq!(r.tube_names(), tubes, "list-tubes");
    for id in 0..(max_put_id + 2).max(MAX_JOB + 48) {
        assert_eq!(
            r.build_stats_job(id, now2),
            want_jobs.get(&id).cloned(),
            "stats-job {id}"
        );
    }
    let fresh = engine(now2, true);
    for t in &tubes {
        let c = tube_counts.get(t).copied().unwrap_or_default();
        let mut want = fresh
            .build_stats_tube(&TubeName::default_tube(), now2)
            .unwrap();
        want.name = t.clone();
        want.current_using = 0;
        want.current_watching = 0;
        want.current_jobs_urgent = c[0];
        want.current_jobs_ready = c[1];
        want.current_jobs_delayed = c[2];
        want.current_jobs_buried = c[3];
        assert_eq!(r.build_stats_tube(t, now2), Some(want), "stats-tube {t}");
    }
    let mut want = fresh.build_stats_server(now2);
    let total = |i: usize| tube_counts.values().map(|c| c[i]).sum::<u64>();
    want.current_jobs_urgent = total(0);
    want.current_jobs_ready = total(1);
    want.current_jobs_delayed = total(2);
    want.current_jobs_buried = total(3);
    want.current_tubes = tubes.len() as u64;
    assert_eq!(r.build_stats_server(now2), want, "stats");
    assert_eq!(r.next_deadline(), next_delay, "next_deadline");

    // Buried FIFO order per tube.
    r.connect(now2, 1);
    for t in &tubes {
        assert_eq!(
            buried_order(&mut r, now2, 1, t),
            buried.get(t).cloned().unwrap_or_default(),
            "buried order of {t}"
        );
    }
    // Ready order, and the next job id.
    let mut r = recover(now2, true, recovery);
    r.connect(now2, 1);
    ready.sort_unstable();
    assert_eq!(
        ready_order(&mut r, now2, 1, &tubes),
        ready.iter().map(|&(_, id)| id).collect::<Vec<_>>(),
        "ready order"
    );
    assert_eq!(put(&mut r, now2, 1, 1, 0, 1, "n"), max_put_id + 1);
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 1_500, ..ProptestConfig::default() })]

    #[test]
    fn journal_replay_recovers_expected_state(
        steps in prop::collection::vec(step(), 20..100),
        restart in restart(),
    ) {
        run_restart(steps, restart);
    }
}
