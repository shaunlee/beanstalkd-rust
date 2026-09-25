//! P3 engine tests: state export / import, determinism and `apply_input`.
//!
//! Inputs are the oracle proptest's random message sequences
//! (`oracle_tests::step`), turned into `EngineInput`s by `Driver`, which
//! follows the server's rules: a connection id is never reused, and no
//! command is sent to a connection that is waiting on `reserve` or has a put
//! in flight. The driver reads those facts from the engine itself, so it
//! also works on an engine restored from an arbitrary (valid) state.
//!
//! * `round_trip_*`: export at a random point (and after every step),
//!   encode with postcard, decode, import, and continue: the restored
//!   engine must be indistinguishable from the original.
//! * `independent_engines_*`: two engines (each with its own `HashMap`
//!   seeds) given the same inputs stay identical, including the snapshot
//!   bytes.
//! * `apply_input_*`: `apply_input` equals the matching method plus `tick`.
//! * `corrupt_*` / `mutated_*`: `import_state` never panics on damaged
//!   snapshots, rejects every field-level corruption that breaks an
//!   invariant, and anything it accepts keeps running with all invariants
//!   intact.

#![allow(clippy::unwrap_used)]

use std::collections::HashSet;

use bytes::Bytes;
use proptest::prelude::*;
use proptest::sample::Index;

use bstk_proto::{Command, PutRejection, TubeName};

use crate::engine::proptests::check_invariants;
use crate::model::{ConnState, JobState, PendingPut, TubeState};
use crate::oracle_tests::{Msg, PutEnd, step};
use crate::{
    ConnId, Engine, EngineConfig, EngineInput, EngineState, JournalEntry, Nanos, Outbox,
    StaticSysInfo,
};

/// Connections that connect before the first random step.
const PRECONNECTED: ConnId = 3;

fn new_engine(journal: bool) -> Engine {
    Engine::new(
        0,
        EngineConfig {
            journal,
            ..EngineConfig::default()
        },
        Box::new(StaticSysInfo::default()),
    )
}

fn sys() -> Box<StaticSysInfo> {
    Box::new(StaticSysInfo::default())
}

fn encode(s: &EngineState) -> Vec<u8> {
    postcard::to_allocvec(s).unwrap()
}

/// Turns random messages into engine inputs the way the server would.
struct Driver {
    now: Nanos,
    ever_connected: HashSet<ConnId>,
}

impl Driver {
    fn new() -> Self {
        Driver {
            now: 0,
            ever_connected: HashSet::new(),
        }
    }

    /// A driver for an engine restored from some state at time `now`.
    fn resume(e: &Engine, now: Nanos) -> Self {
        Driver {
            now,
            ever_connected: e.conn_ids().into_iter().collect(),
        }
    }

    fn can_send(e: &Engine, c: ConnId) -> bool {
        !e.t_conn_waiting(c) && e.t_conn_pending_put(c).is_none()
    }

    /// The inputs for `msg` (possibly none), each applied at `self.now`.
    /// `e` is the engine whose state decides what the server may send.
    fn inputs(&mut self, msg: Msg, tick_after: bool, e: &Engine) -> Vec<EngineInput> {
        let mut v = Vec::new();
        match msg {
            Msg::Connect(c) => {
                if self.ever_connected.insert(c) {
                    v.push(EngineInput::Connect(c));
                }
            }
            Msg::Disconnect(c) => v.push(EngineInput::Disconnect(c)),
            Msg::HalfClose(c) => v.push(EngineInput::HalfClose(c)),
            Msg::PutStarted { conn, too_big } => {
                if Self::can_send(e, conn) {
                    v.push(EngineInput::PutStarted { conn, too_big });
                }
            }
            Msg::Put {
                conn,
                pri,
                delay,
                ttr,
                end,
            } => {
                if !e.t_conn_waiting(conn) {
                    let end = match e.t_conn_pending_put(conn) {
                        Some(true) => PutEnd::TooBig,
                        Some(false) => match end {
                            PutEnd::Body | PutEnd::TooBig => PutEnd::Body,
                            PutEnd::ExpectedCrlf | PutEnd::TrailingGarbage => PutEnd::ExpectedCrlf,
                        },
                        None => end,
                    };
                    let why = match end {
                        PutEnd::Body => None,
                        PutEnd::ExpectedCrlf => Some(PutRejection::ExpectedCrlf),
                        PutEnd::TrailingGarbage => Some(PutRejection::TrailingGarbage),
                        PutEnd::TooBig => Some(PutRejection::JobTooBig),
                    };
                    v.push(match why {
                        None => EngineInput::Command {
                            conn,
                            cmd: Command::Put {
                                pri,
                                delay,
                                ttr,
                                body: Bytes::from_static(b"body"),
                            },
                        },
                        Some(why) => EngineInput::PutRejected { conn, why },
                    });
                }
            }
            Msg::Cmd(conn, cmd) => {
                if Self::can_send(e, conn) {
                    v.push(EngineInput::Command { conn, cmd });
                }
            }
            Msg::Tick => v.push(EngineInput::Tick),
            Msg::AdvanceNs(dt) | Msg::Advance(dt) => {
                self.now += dt;
                if tick_after {
                    v.push(EngineInput::Tick);
                }
            }
            Msg::AdvanceToDeadline(offset) => {
                if let Some(d) = e.next_deadline() {
                    let target = d as i128 + offset as i128;
                    if target > self.now as i128 {
                        self.now = target as Nanos;
                    }
                }
                if tick_after {
                    v.push(EngineInput::Tick);
                }
            }
            Msg::SetDraining(on) => v.push(EngineInput::SetDraining(on)),
        }
        v
    }

    fn preconnect(&mut self) -> Vec<EngineInput> {
        (0..PRECONNECTED)
            .filter(|&c| self.ever_connected.insert(c))
            .map(EngineInput::Connect)
            .collect()
    }
}

/// Applies `input` to `e` and returns the replies and journal entries.
fn apply(e: &mut Engine, now: Nanos, input: EngineInput) -> (Outbox, Vec<JournalEntry>) {
    let mut out = Outbox::new();
    e.apply_input(now, input, &mut out);
    let mut journal = Vec::new();
    e.take_journal(&mut journal);
    (out, journal)
}

/// Feeds every input of `steps` to all `engines` (the first one drives the
/// caller's decisions) and asserts identical replies and journals, plus
/// identical `next_deadline` after each input. `after_step` runs after every
/// step (e.g. to swap in a restored engine).
fn run_lockstep(
    d: &mut Driver,
    engines: &mut [Engine],
    steps: &[(Msg, bool)],
    mut after_step: impl FnMut(&mut [Engine], Nanos),
) {
    for (msg, tick_after) in steps.iter().cloned() {
        let inputs = d.inputs(msg, tick_after, &engines[0]);
        for input in inputs {
            let (out0, j0) = apply(&mut engines[0], d.now, input.clone());
            for e in engines.iter_mut().skip(1) {
                let (out, j) = apply(e, d.now, input.clone());
                assert_eq!(out, out0, "outbox differs for {input:?} at now={}", d.now);
                assert_eq!(j, j0, "journal differs for {input:?}");
            }
            let deadline = engines[0].next_deadline();
            for e in engines.iter().skip(1) {
                assert_eq!(e.next_deadline(), deadline, "next_deadline differs");
            }
        }
        after_step(engines, d.now);
    }
}

/// Export → postcard → decode → import, checking every stage.
fn restore(e: &Engine) -> Engine {
    let s = e.export_state();
    assert_eq!(e.export_state(), s, "export_state is not repeatable");
    let bytes = encode(&s);
    let decoded: EngineState = postcard::from_bytes(&bytes).unwrap();
    assert_eq!(decoded, s, "postcard round trip changed the state");
    let r = match Engine::import_state(decoded, sys()) {
        Ok(r) => r,
        Err(err) => panic!("valid state rejected: {err}"),
    };
    assert_eq!(r.export_state(), s, "import/export changed the state");
    assert_eq!(encode(&r.export_state()), bytes, "snapshot bytes differ");
    r
}

fn round_trip_at(steps: Vec<(Msg, bool)>, split: Index, journal: bool) {
    let k = split.index(steps.len() + 1);
    let mut d = Driver::new();
    let mut a = new_engine(journal);
    for input in d.preconnect() {
        apply(&mut a, d.now, input);
    }
    run_lockstep(&mut d, std::slice::from_mut(&mut a), &steps[..k], |_, _| {});
    let b = restore(&a);
    let mut engines = [a, b];
    run_lockstep(&mut d, &mut engines, &steps[k..], |_, _| {});
    assert_eq!(engines[0].export_state(), engines[1].export_state());
    check_invariants(&engines[1]);
}

fn round_trip_every_step(steps: Vec<(Msg, bool)>, journal: bool) {
    let mut d = Driver::new();
    let mut engines = [new_engine(journal), new_engine(journal)];
    for input in d.preconnect() {
        for e in engines.iter_mut() {
            apply(e, d.now, input.clone());
        }
    }
    run_lockstep(&mut d, &mut engines, &steps, |es, _| {
        es[1] = restore(&es[1]);
    });
    assert_eq!(engines[0].export_state(), engines[1].export_state());
}

fn independent_engines(steps: Vec<(Msg, bool)>, journal: bool) {
    let mut d = Driver::new();
    let mut engines = [new_engine(journal), new_engine(journal)];
    for input in d.preconnect() {
        for e in engines.iter_mut() {
            apply(e, d.now, input.clone());
        }
    }
    run_lockstep(&mut d, &mut engines, &steps, |es, _| {
        assert_eq!(es[0].conn_ids(), es[1].conn_ids());
    });
    let (s0, s1) = (engines[0].export_state(), engines[1].export_state());
    assert_eq!(s0, s1);
    assert_eq!(encode(&s0), encode(&s1), "snapshot bytes differ");
}

/// Runs `input` through the individual engine methods, then `tick`.
fn apply_by_methods(e: &mut Engine, now: Nanos, input: EngineInput, out: &mut Outbox) {
    match input {
        EngineInput::Connect(c) => e.connect(now, c),
        EngineInput::Disconnect(c) => e.disconnect(now, c, out),
        EngineInput::HalfClose(c) => e.half_close(now, c, out),
        EngineInput::PutStarted { conn, too_big } => e.put_started(now, conn, too_big),
        EngineInput::PutRejected { conn, why } => e.put_rejected(now, conn, why, out),
        EngineInput::Command { conn, cmd } => e.handle(now, conn, cmd, out),
        EngineInput::Tick => {}
        EngineInput::SetDraining(on) => e.set_draining(on),
    }
    e.tick(now, out);
}

fn apply_input_matches_methods(steps: Vec<(Msg, bool)>, journal: bool) {
    let mut d = Driver::new();
    let mut a = new_engine(journal);
    let mut b = new_engine(journal);
    let mut inputs = d.preconnect();
    for (msg, tick_after) in steps {
        inputs.extend(d.inputs(msg, tick_after, &a));
        for input in inputs.drain(..) {
            let (out_a, j_a) = apply(&mut a, d.now, input.clone());
            let mut out_b = Outbox::new();
            apply_by_methods(&mut b, d.now, input.clone(), &mut out_b);
            let mut j_b = Vec::new();
            b.take_journal(&mut j_b);
            assert_eq!(out_a, out_b, "outbox differs for {input:?}");
            assert_eq!(j_a, j_b, "journal differs for {input:?}");
            assert_eq!(a.export_state(), b.export_state(), "state differs");
        }
    }
}

/// A valid state after running `steps` (and the time it was taken at).
fn state_after(steps: &[(Msg, bool)], journal: bool) -> (Engine, Nanos) {
    let mut d = Driver::new();
    let mut e = new_engine(journal);
    for input in d.preconnect() {
        apply(&mut e, d.now, input);
    }
    run_lockstep(&mut d, std::slice::from_mut(&mut e), steps, |_, _| {});
    (e, d.now)
}

/// Keeps running an engine accepted by `import_state`; every invariant must
/// hold after each input.
fn keep_running(mut e: Engine, now: Nanos, steps: &[(Msg, bool)]) {
    check_invariants(&e);
    let mut d = Driver::resume(&e, now);
    for (msg, tick_after) in steps.iter().cloned() {
        for input in d.inputs(msg, tick_after, &e) {
            apply(&mut e, d.now, input);
            check_invariants(&e);
        }
    }
}

// ---------------------------------------------------------------------
// Byte-level damage
// ---------------------------------------------------------------------

#[derive(Debug, Clone)]
enum ByteEdit {
    Set(Index, u8),
    FlipBit(Index, u8),
    Insert(Index, u8),
    Remove(Index),
    Truncate(Index),
}

fn byte_edit() -> impl Strategy<Value = ByteEdit> {
    prop_oneof![
        3 => (any::<Index>(), any::<u8>()).prop_map(|(i, b)| ByteEdit::Set(i, b)),
        3 => (any::<Index>(), 0..8u8).prop_map(|(i, b)| ByteEdit::FlipBit(i, b)),
        1 => (any::<Index>(), any::<u8>()).prop_map(|(i, b)| ByteEdit::Insert(i, b)),
        1 => any::<Index>().prop_map(ByteEdit::Remove),
        1 => any::<Index>().prop_map(ByteEdit::Truncate),
    ]
}

fn apply_byte_edit(bytes: &mut Vec<u8>, edit: &ByteEdit) {
    if bytes.is_empty() {
        return;
    }
    match edit {
        ByteEdit::Set(i, b) => {
            let i = i.index(bytes.len());
            bytes[i] = *b;
        }
        ByteEdit::FlipBit(i, bit) => {
            let i = i.index(bytes.len());
            bytes[i] ^= 1 << bit;
        }
        ByteEdit::Insert(i, b) => {
            let i = i.index(bytes.len() + 1);
            bytes.insert(i, *b);
        }
        ByteEdit::Remove(i) => {
            let i = i.index(bytes.len());
            bytes.remove(i);
        }
        ByteEdit::Truncate(i) => {
            let i = i.index(bytes.len());
            bytes.truncate(i);
        }
    }
}

fn mutated_snapshot(steps: Vec<(Msg, bool)>, edits: Vec<ByteEdit>, more: Vec<(Msg, bool)>) {
    let (e, now) = state_after(&steps, false);
    let mut bytes = encode(&e.export_state());
    for edit in &edits {
        apply_byte_edit(&mut bytes, edit);
    }
    let Ok(state) = postcard::from_bytes::<EngineState>(&bytes) else {
        return;
    };
    if let Ok(r) = Engine::import_state(state, sys()) {
        keep_running(r, now, &more);
    }
}

// ---------------------------------------------------------------------
// Field-level damage
// ---------------------------------------------------------------------

/// A targeted corruption of an `EngineState`. Each one, whenever it changes
/// the state at all, breaks at least one invariant.
#[derive(Debug, Clone)]
enum Corruption {
    /// Add `delta` (non-zero) to a global recomputable counter.
    GlobalCounter(u8, u64),
    /// Add 1 to a per-tube recomputable counter, or set `job_ref_ct` to
    /// `u64::MAX`.
    TubeCounter(Index, u8),
    /// Remove one entry of `conn_ticks` / `delay_heads` / `pauses` /
    /// `dispatchable`.
    DropIndexEntry(u8, Index),
    /// Insert an entry naming a missing tube or connection into an index.
    BogusIndexEntry(u8),
    JobInMissingTube(Index),
    JobState(Index, u8),
    JobReservedByMissingConn(Index),
    JobTtrZero(Index),
    ReadyJobPri(Index),
    DropJob(Index),
    DuplicateJob(Index),
    NextIdTooLow,
    DefaultMissing,
    RenameTube(Index),
    SwapTubeOrder(Index, Index),
    TubePos(Index),
    FreeListLiveTube(Index),
    FreeListOutOfRange,
    /// Replace a free-list entry with a copy of another one.
    FreeListDuplicate,
    /// Move `default` away from the head of the tube list, keeping the
    /// positions consistent.
    DefaultNotFirst(Index),
    DropTubeId(Index),
    FlipWaiting(Index),
    DuplicateWatch(Index),
    WatchMissingTube(Index),
    UseMissingTube(Index),
    ClearWatch(Index),
    WaitDeadlineNotWaiting(Index),
    DuplicateReservation(Index),
    DropReservation(Index),
    TickKey(Index),
    DelayHead(Index),
    DispatchableFlag(Index),
    PendingPutLiveJob(Index, Index),
    PendingPutFutureId(Index),
    DuplicateBuried(Index),
    DropReady(Index),
    DropDelayed(Index),
    DropBuried(Index),
    DropWaitingConn(Index),
    SwapConns,
    ProducerFlag(Index),
}

fn corruption() -> impl Strategy<Value = Corruption> {
    use Corruption as C;
    let i = any::<Index>;
    prop_oneof![
        (0..9u8, prop_oneof![1..5u64, Just(u64::MAX)]).prop_map(|(w, d)| C::GlobalCounter(w, d)),
        (i(), 0..8u8).prop_map(|(t, w)| C::TubeCounter(t, w)),
        (0..4u8, i()).prop_map(|(w, x)| C::DropIndexEntry(w, x)),
        (0..4u8).prop_map(C::BogusIndexEntry),
        i().prop_map(C::JobInMissingTube),
        (i(), 1..4u8).prop_map(|(j, s)| C::JobState(j, s)),
        i().prop_map(C::JobReservedByMissingConn),
        i().prop_map(C::JobTtrZero),
        i().prop_map(C::ReadyJobPri),
        i().prop_map(C::DropJob),
        i().prop_map(C::DuplicateJob),
        Just(C::NextIdTooLow),
        Just(C::DefaultMissing),
        i().prop_map(C::RenameTube),
        (i(), i()).prop_map(|(a, b)| C::SwapTubeOrder(a, b)),
        i().prop_map(C::TubePos),
        i().prop_map(C::FreeListLiveTube),
        Just(C::FreeListOutOfRange),
        Just(C::FreeListDuplicate),
        i().prop_map(C::DefaultNotFirst),
        i().prop_map(C::DropTubeId),
        i().prop_map(C::FlipWaiting),
        i().prop_map(C::DuplicateWatch),
        i().prop_map(C::WatchMissingTube),
        i().prop_map(C::UseMissingTube),
        i().prop_map(C::ClearWatch),
        i().prop_map(C::WaitDeadlineNotWaiting),
        i().prop_map(C::DuplicateReservation),
        i().prop_map(C::DropReservation),
        i().prop_map(C::TickKey),
        i().prop_map(C::DelayHead),
        i().prop_map(C::DispatchableFlag),
        (i(), i()).prop_map(|(c, j)| C::PendingPutLiveJob(c, j)),
        i().prop_map(C::PendingPutFutureId),
        i().prop_map(C::DuplicateBuried),
        i().prop_map(C::DropReady),
        i().prop_map(C::DropDelayed),
        i().prop_map(C::DropBuried),
        i().prop_map(C::DropWaitingConn),
        Just(C::SwapConns),
        i().prop_map(C::ProducerFlag),
    ]
}

/// Ids of the live tube slots.
fn live_tubes(s: &EngineState) -> Vec<usize> {
    s.tubes
        .iter()
        .enumerate()
        .filter_map(|(i, t)| t.as_ref().map(|_| i))
        .collect()
}

fn pick<T: Copy>(v: &[T], i: &Index) -> Option<T> {
    (!v.is_empty()).then(|| v[i.index(v.len())])
}

fn tube_at<'a>(s: &'a mut EngineState, i: &Index) -> Option<&'a mut TubeState> {
    let t = pick(&live_tubes(s), i)?;
    s.tubes[t].as_mut()
}

fn conn_at<'a>(s: &'a mut EngineState, i: &Index) -> Option<&'a mut ConnState> {
    let n = s.conns.len();
    if n == 0 {
        return None;
    }
    Some(&mut s.conns[i.index(n)].1)
}

fn remove_nth<T: Ord + Clone>(set: &mut std::collections::BTreeSet<T>, i: &Index) {
    if !set.is_empty() {
        let x = set.iter().nth(i.index(set.len())).cloned().unwrap();
        set.remove(&x);
    }
}

fn corrupt(s: &mut EngineState, c: &Corruption) {
    use Corruption as C;
    let missing_tube = s.tubes.len() + 3;
    let missing_conn = ConnId::MAX - 1;
    match c {
        C::GlobalCounter(w, d) => {
            let d32 = (*d as u32).max(1);
            match w {
                0 => s.ready_ct = s.ready_ct.wrapping_add(*d),
                1 => s.urgent_ct = s.urgent_ct.wrapping_add(*d),
                2 => s.reserved_ct = s.reserved_ct.wrapping_add(*d),
                3 => s.buried_ct = s.buried_ct.wrapping_add(*d),
                4 => s.delayed_ct = s.delayed_ct.wrapping_add(*d),
                5 => s.waiting_ct = s.waiting_ct.wrapping_add(*d),
                6 => s.cur_conns = s.cur_conns.wrapping_add(d32),
                7 => s.cur_producers = s.cur_producers.wrapping_add(d32),
                _ => s.cur_workers = s.cur_workers.wrapping_add(d32),
            }
        }
        C::TubeCounter(t, w) => {
            if let Some(t) = tube_at(s, t) {
                match w {
                    0 => t.stat.urgent_ct += 1,
                    1 => t.stat.buried_ct += 1,
                    2 => t.stat.reserved_ct += 1,
                    3 => t.stat.waiting_ct += 1,
                    4 => t.using_ct += 1,
                    5 => t.watching_ct += 1,
                    6 => t.job_ref_ct += 1,
                    _ => t.job_ref_ct = u64::MAX,
                }
            }
        }
        C::DropIndexEntry(w, i) => match w {
            0 => remove_nth(&mut s.conn_ticks, i),
            1 => remove_nth(&mut s.delay_heads, i),
            2 => remove_nth(&mut s.pauses, i),
            _ => remove_nth(&mut s.dispatchable, i),
        },
        C::BogusIndexEntry(w) => {
            match w {
                0 => s.conn_ticks.insert((7, missing_conn)),
                1 => s.delay_heads.insert((7, missing_tube)),
                2 => s.pauses.insert((7, missing_tube)),
                _ => s.dispatchable.insert(missing_tube),
            };
        }
        C::JobInMissingTube(j) => {
            if !s.jobs.is_empty() {
                let n = s.jobs.len();
                s.jobs[j.index(n)].tube = missing_tube;
            }
        }
        C::JobState(j, shift) => {
            if !s.jobs.is_empty() {
                let n = s.jobs.len();
                let job = &mut s.jobs[j.index(n)];
                let all = [
                    JobState::Ready,
                    JobState::Reserved,
                    JobState::Delayed,
                    JobState::Buried,
                ];
                let cur = all.iter().position(|&x| x == job.state).unwrap();
                job.state = all[(cur + *shift as usize) % all.len()];
            }
        }
        C::JobReservedByMissingConn(j) => {
            if !s.jobs.is_empty() {
                let n = s.jobs.len();
                s.jobs[j.index(n)].reserver = Some(missing_conn);
            }
        }
        C::JobTtrZero(j) => {
            if !s.jobs.is_empty() {
                let n = s.jobs.len();
                s.jobs[j.index(n)].ttr = 0;
            }
        }
        C::ReadyJobPri(j) => {
            if !s.jobs.is_empty() {
                let n = s.jobs.len();
                let job = &mut s.jobs[j.index(n)];
                if job.state == JobState::Ready {
                    job.pri ^= 1;
                }
            }
        }
        C::DropJob(j) => {
            if !s.jobs.is_empty() {
                let n = s.jobs.len();
                s.jobs.remove(j.index(n));
            }
        }
        C::DuplicateJob(j) => {
            if !s.jobs.is_empty() {
                let k = j.index(s.jobs.len());
                let copy = s.jobs[k].clone();
                s.jobs.insert(k, copy);
            }
        }
        C::NextIdTooLow => {
            s.next_job_id = s.jobs.iter().map(|j| j.id).max().unwrap_or(0);
        }
        C::DefaultMissing => s.tubes[0] = None,
        C::RenameTube(t) => {
            if let Some(t) = tube_at(s, t) {
                t.name = TubeName::new("corrupt-name").unwrap();
            }
        }
        C::SwapTubeOrder(a, b) => {
            let n = s.tube_order.items.len();
            if n > 0 {
                s.tube_order.items.swap(a.index(n), b.index(n));
            }
        }
        C::TubePos(t) => {
            if let Some(t) = tube_at(s, t) {
                t.pos += 1;
            }
        }
        C::FreeListLiveTube(t) => {
            if let Some(t) = pick(&live_tubes(s), t) {
                s.free_tube_ids.push(t);
            }
        }
        C::FreeListOutOfRange => s.free_tube_ids.push(missing_tube),
        C::FreeListDuplicate => {
            let n = s.free_tube_ids.len();
            if n >= 2 {
                s.free_tube_ids[n - 1] = s.free_tube_ids[0];
            }
        }
        C::DefaultNotFirst(i) => {
            let n = s.tube_order.items.len();
            if n >= 2 {
                let k = 1 + i.index(n - 1);
                let other = s.tube_order.items[k];
                s.tube_order.items.swap(0, k);
                if let Some(t) = s.tubes[0].as_mut() {
                    t.pos = k;
                }
                if let Some(t) = s.tubes[other].as_mut() {
                    t.pos = 0;
                }
            }
        }
        C::DropTubeId(i) => {
            if !s.tube_ids.is_empty() {
                let n = s.tube_ids.len();
                s.tube_ids.remove(i.index(n));
            }
        }
        _ => corrupt_conn_or_set(s, c, missing_tube),
    }
}

fn corrupt_conn_or_set(s: &mut EngineState, c: &Corruption, missing_tube: usize) {
    use Corruption as C;
    match c {
        C::FlipWaiting(i) => {
            if let Some(c) = conn_at(s, i) {
                c.waiting = !c.waiting;
            }
        }
        C::DuplicateWatch(i) => {
            if let Some(c) = conn_at(s, i)
                && let Some(&t) = c.watch.items.first()
            {
                c.watch.items.push(t);
            }
        }
        C::WatchMissingTube(i) => {
            if let Some(c) = conn_at(s, i) {
                c.watch.items.push(missing_tube);
            }
        }
        C::UseMissingTube(i) => {
            if let Some(c) = conn_at(s, i) {
                c.use_tube = missing_tube;
            }
        }
        C::ClearWatch(i) => {
            if let Some(c) = conn_at(s, i) {
                c.watch.items.clear();
            }
        }
        C::WaitDeadlineNotWaiting(i) => {
            if let Some(c) = conn_at(s, i)
                && !c.waiting
            {
                c.wait_deadline = Some(5);
            }
        }
        C::DuplicateReservation(i) => {
            if let Some(c) = conn_at(s, i)
                && let Some(&id) = c.reserved_fifo.first()
            {
                c.reserved_fifo.push(id);
            }
        }
        C::DropReservation(i) => {
            if let Some(c) = conn_at(s, i)
                && !c.reserved_fifo.is_empty()
            {
                c.reserved_fifo.remove(0);
            }
        }
        C::TickKey(i) => {
            if let Some(c) = conn_at(s, i) {
                c.tick_key = match c.tick_key {
                    Some(_) => None,
                    None => Some(12_345),
                };
            }
        }
        C::DelayHead(i) => {
            if let Some(t) = tube_at(s, i) {
                t.delay_head = match t.delay_head {
                    Some(d) => Some(d.wrapping_add(1)),
                    None => Some(12_345),
                };
            }
        }
        C::DispatchableFlag(i) => {
            if let Some(t) = tube_at(s, i) {
                t.dispatchable = !t.dispatchable;
            }
        }
        C::PendingPutLiveJob(i, j) => {
            let ids: Vec<u64> = s.jobs.iter().map(|j| j.id).collect();
            if let Some(id) = pick(&ids, j)
                && let Some(c) = conn_at(s, i)
            {
                c.pending_put = Some(PendingPut {
                    id: Some(id),
                    created_at: 0,
                });
            }
        }
        C::PendingPutFutureId(i) => {
            let next = s.next_job_id;
            if let Some(c) = conn_at(s, i) {
                c.pending_put = Some(PendingPut {
                    id: Some(next),
                    created_at: 0,
                });
            }
        }
        C::DuplicateBuried(i) => {
            if let Some(t) = tube_at(s, i)
                && let Some(&id) = t.buried.front()
            {
                t.buried.push_back(id);
            }
        }
        C::DropReady(i) => {
            if let Some(t) = tube_at(s, i) {
                t.ready.pop_first();
            }
        }
        C::DropDelayed(i) => {
            if let Some(t) = tube_at(s, i) {
                t.delayed.pop_first();
            }
        }
        C::DropBuried(i) => {
            if let Some(t) = tube_at(s, i) {
                t.buried.pop_front();
            }
        }
        C::DropWaitingConn(i) => {
            if let Some(t) = tube_at(s, i)
                && !t.waiting_conns.is_empty()
            {
                t.waiting_conns.remove_at(0);
            }
        }
        C::SwapConns => {
            if s.conns.len() >= 2 {
                s.conns.swap(0, 1);
            }
        }
        C::ProducerFlag(i) => {
            if let Some(c) = conn_at(s, i) {
                c.is_producer = !c.is_producer;
            }
        }
        _ => unreachable!("handled by corrupt"),
    }
}

/// Applies one corruption (several could cancel out, e.g. two producer
/// flags flipped in opposite directions).
fn corrupt_field(steps: Vec<(Msg, bool)>, corruption: Corruption) {
    let (e, _) = state_after(&steps, false);
    let original = e.export_state();
    let mut s = original.clone();
    corrupt(&mut s, &corruption);
    if s == original {
        // Nothing to corrupt in this state (e.g. no jobs).
        return;
    }
    let bytes = encode(&s);
    if let Ok(r) = Engine::import_state(s, sys()) {
        panic!(
            "corrupt state accepted: {corruption:?}\n{:?}",
            r.export_state()
        );
    }
    // The encoded form of a rejected state is rejected too.
    let decoded: Result<EngineState, _> = postcard::from_bytes(&bytes);
    if let Ok(d) = decoded {
        assert!(Engine::import_state(d, sys()).is_err());
    }
}

/// Changes to history fields the validator cannot recompute are accepted
/// and kept.
fn benign_edit(steps: Vec<(Msg, bool)>, which: u8, v: u64) {
    let (e, _) = state_after(&steps, false);
    let mut s = e.export_state();
    match which {
        0 => s.cmd_put = v,
        1 => s.total_jobs_ct = v,
        2 => s.start = v,
        3 => s.draining = !s.draining,
        4 => s.binlog.records_written = v,
        5 => s.timeout_ct = v,
        6 => s.tube_order.last = v as usize,
        _ => {
            if let Some(t) = s.tubes[0].as_mut() {
                t.stat.total_delete_ct = v;
            }
        }
    }
    let r = match Engine::import_state(s.clone(), sys()) {
        Ok(r) => r,
        Err(err) => panic!("benign edit {which} rejected: {err}"),
    };
    assert_eq!(r.export_state(), s);
}

fn steps(min: usize, max: usize) -> impl Strategy<Value = Vec<(Msg, bool)>> {
    prop::collection::vec(step(), min..max)
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 3_000, ..ProptestConfig::default() })]

    #[test]
    fn round_trip_at_random_point_is_invisible(
        steps in steps(20, 160),
        split in any::<Index>(),
        journal in any::<bool>(),
    ) {
        round_trip_at(steps, split, journal);
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 1_000, ..ProptestConfig::default() })]

    #[test]
    fn round_trip_after_every_step_is_invisible(
        steps in steps(20, 100),
        journal in any::<bool>(),
    ) {
        round_trip_every_step(steps, journal);
    }

    #[test]
    fn independent_engines_stay_identical(
        steps in steps(20, 120),
        journal in any::<bool>(),
    ) {
        independent_engines(steps, journal);
    }

    #[test]
    fn apply_input_equals_method_then_tick(
        steps in steps(20, 100),
        journal in any::<bool>(),
    ) {
        apply_input_matches_methods(steps, journal);
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 3_000, ..ProptestConfig::default() })]

    #[test]
    fn mutated_snapshot_bytes_never_panic(
        steps in steps(10, 80),
        edits in prop::collection::vec(byte_edit(), 1..4),
        more in steps(0, 30),
    ) {
        mutated_snapshot(steps, edits, more);
    }

    #[test]
    fn corrupt_fields_are_rejected(
        steps in steps(10, 80),
        corruption in corruption(),
    ) {
        corrupt_field(steps, corruption);
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 500, ..ProptestConfig::default() })]

    #[test]
    fn benign_history_edits_are_accepted(
        steps in steps(10, 60),
        which in 0..8u8,
        v in any::<u64>(),
    ) {
        benign_edit(steps, which, v);
    }

    #[test]
    fn random_bytes_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..400)) {
        if let Ok(s) = postcard::from_bytes::<EngineState>(&bytes) {
            let _ = Engine::import_state(s, sys());
        }
    }
}

#[test]
fn fresh_engine_round_trips() {
    for journal in [false, true] {
        let e = new_engine(journal);
        let r = restore(&e);
        assert_eq!(r.config(), e.config());
        check_invariants(&r);
    }
}

#[test]
fn empty_and_truncated_snapshots_are_rejected_without_panic() {
    let (e, _) = state_after(&[], false);
    let bytes = encode(&e.export_state());
    for len in 0..bytes.len() {
        if let Ok(s) = postcard::from_bytes::<EngineState>(&bytes[..len]) {
            let _ = Engine::import_state(s, sys());
        }
    }
}

#[test]
fn restored_engine_keeps_reservations_waiters_and_pauses() {
    use bstk_proto::Response;
    const SEC: Nanos = crate::NANOS_PER_SEC;
    let mut e = new_engine(false);
    let mut out = Outbox::new();
    for c in 1..=3 {
        e.apply_input(0, EngineInput::Connect(c), &mut out);
    }
    let put = |ttr| EngineInput::Command {
        conn: 1,
        cmd: Command::Put {
            pri: 5,
            delay: 0,
            ttr,
            body: Bytes::from_static(b"x"),
        },
    };
    e.apply_input(0, put(10), &mut out);
    e.apply_input(
        0,
        EngineInput::Command {
            conn: 2,
            cmd: Command::Reserve,
        },
        &mut out,
    );
    e.apply_input(
        0,
        EngineInput::Command {
            conn: 3,
            cmd: Command::ReserveWithTimeout(20),
        },
        &mut out,
    );
    e.apply_input(
        0,
        EngineInput::Command {
            conn: 1,
            cmd: Command::PauseTube {
                tube: TubeName::default_tube(),
                delay: 3,
            },
        },
        &mut out,
    );
    out.clear();
    let mut r = restore(&e);
    assert_eq!(r.next_deadline(), e.next_deadline());
    // Job 1 (reserved by 2) times out at 10 s and goes to waiter 3.
    r.apply_input(10 * SEC + 1, EngineInput::Tick, &mut out);
    assert_eq!(
        out,
        vec![(
            3,
            Response::Reserved {
                id: 1,
                body: Bytes::from_static(b"x")
            }
        )]
    );
}

#[test]
fn duplicate_free_slot_is_rejected() {
    let mut e = new_engine(false);
    let mut out = Outbox::new();
    e.apply_input(0, EngineInput::Connect(1), &mut out);
    for cmd in [
        Command::Watch(TubeName::new("a").unwrap()),
        Command::Watch(TubeName::new("b").unwrap()),
        Command::Ignore(TubeName::new("a").unwrap()),
        Command::Ignore(TubeName::new("b").unwrap()),
    ] {
        e.apply_input(0, EngineInput::Command { conn: 1, cmd }, &mut out);
    }
    let mut s = e.export_state();
    assert_eq!(s.free_tube_ids.len(), 2);
    assert!(Engine::import_state(s.clone(), sys()).is_ok());
    s.free_tube_ids[1] = s.free_tube_ids[0];
    assert!(Engine::import_state(s, sys()).is_err());
}

/// L1: an absurd tube slab is rejected before the per-tube arrays of the
/// validation are allocated; a large legitimate one (many freed tubes) is
/// accepted. (Unit tests use a cap of 4096 slots; the real one is 2^24.)
#[test]
fn absurd_tube_slab_is_rejected() {
    let mut e = new_engine(false);
    let mut out = Outbox::new();
    e.apply_input(0, EngineInput::Connect(1), &mut out);
    // 1000 tubes created and freed again: a legitimate state with far more
    // free slots than live tubes.
    let names: Vec<TubeName> = (0..1000)
        .map(|i| TubeName::new(&format!("t{i}")).unwrap())
        .collect();
    for cmd in names.iter().cloned().map(Command::Watch) {
        e.apply_input(0, EngineInput::Command { conn: 1, cmd }, &mut out);
    }
    for cmd in names.into_iter().map(Command::Ignore) {
        e.apply_input(0, EngineInput::Command { conn: 1, cmd }, &mut out);
    }
    let s = e.export_state();
    assert_eq!(s.free_tube_ids.len(), 1000);
    assert!(Engine::import_state(s.clone(), sys()).is_ok());
    let mut big = s.clone();
    for _ in 0..3 {
        // Grow the slab with consistent free slots.
        let n = big.tubes.len();
        big.tubes.extend((0..2000).map(|_| None));
        big.free_tube_ids.extend(n..n + 2000);
    }
    let err = Engine::import_state(big, sys()).map(|_| ()).unwrap_err();
    assert!(err.to_string().contains("tube slab"), "{err}");
}
