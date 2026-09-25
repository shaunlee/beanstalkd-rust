//! State machine: dedup rules, DropNode, reply routing, determinism,
//! snapshots and restarts.

use bstk_engine::{Engine, NANOS_PER_SEC, Outbox};
use bstk_proto::{Command, TubeName};
use openraft::storage::RaftStateMachine;
use openraft::{BasicNode, Membership, RaftSnapshotBuilder, StoredMembership};
use tempfile::TempDir;

use super::*;
use crate::storage::snapshot::crash_point::{self, Point};
use crate::storage::state_machine::SnapshotPayload;
use crate::{Applied, conn_id, owner_of};

struct Node {
    _dir: TempDir,
    sm: ClusterStateMachine,
    sink: Arc<RecSink>,
}

fn node(id: NodeId) -> Node {
    let dir = tempfile::tempdir().unwrap();
    let sink = Arc::new(RecSink::default());
    let sm = open_sm(dir.path(), id, sink.clone());
    Node {
        _dir: dir,
        sm,
        sink,
    }
}

fn req(now: u64, op: Op) -> Request {
    Request { now, op }
}

fn c_in(seq: u64, input: EngineInput) -> Op {
    Op::Conn { seq, input }
}

fn cmd(conn: ConnId, cmd: Command) -> EngineInput {
    EngineInput::Command { conn, cmd }
}

fn put(conn: ConnId, body: &str) -> EngineInput {
    cmd(
        conn,
        Command::Put {
            pri: 1,
            delay: 0,
            ttr: 60,
            body: body.as_bytes().to_vec().into(),
        },
    )
}

fn tube(n: &str) -> TubeName {
    TubeName::new(n).unwrap()
}

/// Entries `first..` holding `reqs`.
fn entries(first: u64, reqs: &[Request]) -> Vec<Entry<TypeConfig>> {
    reqs.iter()
        .enumerate()
        .map(|(i, r)| normal(1, first + i as u64, r.clone()))
        .collect()
}

fn apply(sm: &mut ClusterStateMachine, ents: Vec<Entry<TypeConfig>>) -> Vec<Applied> {
    block_on(sm.apply(ents)).unwrap()
}

fn dups(res: &[Applied]) -> Vec<bool> {
    res.iter().map(|a| a.duplicate).collect()
}

fn state_bytes(sm: &ClusterStateMachine) -> Vec<u8> {
    postcard::to_allocvec(&sm.handle().export_state().unwrap()).unwrap()
}

const S: u64 = NANOS_PER_SEC;

#[test]
fn dedup_rules() {
    let mut n = node(1);
    let a = conn_id(1, 1);
    let b = conn_id(1, 3);
    let late = conn_id(1, 2);
    let d = conn_id(1, 10);
    let reqs = [
        req(S, c_in(1, EngineInput::Connect(a))),         // applied
        req(S, c_in(1, EngineInput::Connect(a))),         // dup connect
        req(S, c_in(2, cmd(a, Command::Use(tube("x"))))), // applied
        req(S, c_in(2, cmd(a, Command::Use(tube("y"))))), // resend: dup
        req(S, c_in(4, cmd(a, Command::ListTubeUsed))),   // gap: ignored
        req(S, c_in(3, cmd(a, Command::ListTubeUsed))),   // applied
        req(S, c_in(1, EngineInput::Connect(b))),         // applied
        req(S, c_in(1, EngineInput::Connect(late))),      // local 2 <= 3: dup
        req(S, c_in(4, EngineInput::Disconnect(a))),      // applied
        req(S, c_in(1, EngineInput::Connect(a))),         // late connect: dup
        req(S, c_in(5, cmd(a, Command::ListTubeUsed))),   // closed conn: dup
        req(S, c_in(9, EngineInput::Tick)),               // no connection: ignored
        req(S, c_in(2, EngineInput::Connect(d))),         // connect must be seq 1
        req(S, c_in(1, EngineInput::Connect(d))),         // applied
        req(S, c_in(2, EngineInput::HalfClose(d))),       // applied
    ];
    let res = apply(&mut n.sm, entries(1, &reqs));
    assert_eq!(
        dups(&res),
        vec![
            false, true, false, true, true, false, false, true, false, true, true, true, true,
            false, false
        ]
    );
    assert_eq!(
        n.sink.take(),
        vec![
            Ev::Applied(a, 1),
            Ev::Applied(a, 2),
            Ev::Deliver(a, Response::Using(tube("x"))),
            Ev::Applied(a, 3),
            Ev::Deliver(a, Response::Using(tube("x"))),
            Ev::Applied(b, 1),
            Ev::Applied(a, 4),
            Ev::Closed(a),
            Ev::Applied(d, 1),
            Ev::Applied(d, 2),
        ]
    );
    let h = n.sm.handle();
    assert_eq!(h.conn_ids(), vec![b, d]);
    assert_eq!(h.applied_seq(a), None);
    assert_eq!(h.applied_seq(b), Some(1));
    assert_eq!(h.applied_seq(d), Some(2));
    assert_eq!(h.highest_local(1), 10);
    assert_eq!(h.highest_local(2), 0);
    assert_eq!(h.last_applied(), Some(lid(1, reqs.len() as u64)));
}

/// An owner resends its unapplied inputs after a leader change; the new
/// leader's log may also hold the old leader's copies. Each input is
/// applied, and answered, exactly once.
#[test]
fn resend_after_leader_change() {
    let mut n = node(2);
    let c = conn_id(2, 7);
    let first = [
        req(S, c_in(1, EngineInput::Connect(c))),
        req(S, c_in(2, put(c, "one"))),
    ];
    apply(&mut n.sm, entries(1, &first));
    // New leader: the owner resends seq 2 (already applied) and 3; the
    // old leader's copy of 3 was also committed.
    let second = [
        req(2 * S, c_in(2, put(c, "one"))),
        req(2 * S, c_in(3, put(c, "two"))),
        req(2 * S, c_in(3, put(c, "two"))),
        req(2 * S, c_in(4, cmd(c, Command::Delete(1)))),
    ];
    let res = apply(&mut n.sm, entries(3, &second));
    assert_eq!(dups(&res), vec![true, false, true, false]);
    assert_eq!(
        n.sink.take(),
        vec![
            Ev::Applied(c, 1),
            Ev::Applied(c, 2),
            Ev::Deliver(c, Response::Inserted(1)),
            Ev::Applied(c, 3),
            Ev::Deliver(c, Response::Inserted(2)),
            Ev::Applied(c, 4),
            Ev::Deliver(c, Response::Deleted),
        ]
    );
    let snap = n.sm.handle().snapshot_limited(3 * S, 10).unwrap();
    assert_eq!(snap.server.total_jobs, 2);
    assert_eq!(snap.server.current_jobs_ready, 1);
}

/// `DropNode { node: n, .. }` disconnects exactly `n`'s connections (up to
/// the bound), lowest id first,
/// releasing their reservations (which can wake other nodes' waiters).
#[test]
fn drop_node_disconnects_owner_connections_in_order() {
    let p = conn_id(3, 1); // producer on node 3
    let w = conn_id(3, 2); // waiter on node 3
    let r_lo = conn_id(2, 3);
    let r_hi = conn_id(2, 5);
    let other = conn_id(1, 1);
    let reqs = [
        req(S, c_in(1, EngineInput::Connect(p))),
        req(S, c_in(1, EngineInput::Connect(r_lo))),
        req(S, c_in(1, EngineInput::Connect(r_hi))),
        req(S, c_in(1, EngineInput::Connect(other))),
        req(S, c_in(2, put(p, "a"))),
        req(S, c_in(3, put(p, "b"))),
        // r_hi reserves job 1, r_lo reserves job 2.
        req(S, c_in(2, cmd(r_hi, Command::Reserve))),
        req(S, c_in(2, cmd(r_lo, Command::Reserve))),
        req(S, c_in(1, EngineInput::Connect(w))),
        req(S, c_in(2, cmd(w, Command::Reserve))), // waits
        req(
            2 * S,
            Op::DropNode {
                node: 2,
                up_to_local: 5,
            },
        ),
        req(2 * S, c_in(1, EngineInput::Connect(conn_id(2, 4)))), // dup
        req(2 * S, c_in(3, cmd(r_lo, Command::ListTubeUsed))),    // dup
        req(2 * S, c_in(1, EngineInput::Connect(conn_id(2, 6)))), // new
    ];
    let mut ns: Vec<Node> = (1..=3).map(node).collect();
    for n in &mut ns {
        let res = apply(&mut n.sm, entries(1, &reqs));
        assert_eq!(
            dups(&res)[10..],
            [false, true, true, false],
            "node {}",
            n.sm.handle().node_id()
        );
    }
    let ev2 = ns[1].sink.take();
    // Node 2 is told to close its connections, ascending.
    let closed: Vec<ConnId> = ev2
        .iter()
        .filter_map(|e| match e {
            Ev::Closed(c) => Some(*c),
            _ => None,
        })
        .collect();
    assert_eq!(closed, vec![r_lo, r_hi]);
    // Node 3's waiter gets the job released first: r_lo's job 2.
    let ev3 = ns[2].sink.take();
    let waiter: Vec<&Ev> = ev3
        .iter()
        .filter(|e| matches!(e, Ev::Deliver(c, _) if *c == w))
        .collect();
    assert_eq!(
        waiter,
        vec![&Ev::Deliver(
            w,
            Response::Reserved {
                id: 2,
                body: "b".as_bytes().to_vec().into()
            }
        )]
    );
    // Node 1 heard nothing about other nodes' connections.
    let ev1 = ns[0].sink.take();
    assert_eq!(ev1, vec![Ev::Applied(other, 1)]);
    for n in &ns {
        let h = n.sm.handle();
        assert_eq!(h.conn_ids(), vec![other, conn_id(2, 6), p, w]);
        assert_eq!(h.applied_seq(r_lo), None);
        assert_eq!(h.highest_local(2), 6);
        let snap = h.snapshot_limited(2 * S, 10).unwrap();
        assert_eq!(snap.server.current_jobs_reserved, 1);
        assert_eq!(snap.server.current_jobs_ready, 1);
    }
    assert_eq!(state_bytes(&ns[0].sm), state_bytes(&ns[1].sm));
    assert_eq!(state_bytes(&ns[0].sm), state_bytes(&ns[2].sm));
}

/// A deterministic pseudo-random workload over connections of three nodes.
/// A probe engine tracks which connections await a reply, so a connection
/// never has two commands in flight (as the server guarantees).
fn workload(len: usize, seed: u64) -> Vec<Request> {
    let mut x = seed | 1;
    let mut rnd = move |n: u64| {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x % n
    };
    let mut out = Vec::new();
    let mut now = S;
    let mut next_local = [0u64; 4];
    let mut conns: Vec<(ConnId, u64)> = Vec::new(); // (conn, next seq)
    let mut awaiting: std::collections::BTreeSet<ConnId> = Default::default();
    let mut probe: Option<Engine> = None;
    let mut last_now = 0;
    let tubes = ["default", "a", "b"];
    for _ in 0..len {
        now += rnd(S / 2);
        // Sometimes time goes backwards (clock skew); the SM clamps.
        let stamp = if rnd(20) == 0 { now - S / 4 } else { now };
        let mut dup = false;
        let op = match rnd(100) {
            0..=9 if conns.len() < 12 => {
                let owner = 1 + rnd(3);
                next_local[owner as usize] += 1 + rnd(2);
                let c = conn_id(owner, next_local[owner as usize]);
                conns.push((c, 2));
                c_in(1, EngineInput::Connect(c))
            }
            10..=11 if !conns.is_empty() => {
                let i = rnd(conns.len() as u64) as usize;
                let (c, s) = conns.remove(i);
                awaiting.remove(&c);
                c_in(s, EngineInput::Disconnect(c))
            }
            12 => Op::DropNode {
                node: 1 + rnd(3),
                up_to_local: if rnd(2) == 0 { u64::MAX } else { rnd(20) },
            },
            13..=15 => Op::Tick,
            16 => Op::SetDraining(rnd(4) == 0),
            _ if !conns.is_empty() => {
                let i = rnd(conns.len() as u64) as usize;
                let (c, s) = conns[i];
                dup = rnd(10) == 0 && s > 2;
                let seq = if dup { s - 1 } else { s };
                if !dup {
                    conns[i].1 += 1;
                }
                let t = tube(tubes[rnd(3) as usize]);
                let id = 1 + rnd(20);
                let input = if awaiting.contains(&c) || rnd(12) == 11 {
                    EngineInput::HalfClose(c)
                } else {
                    match rnd(11) {
                        0..=2 => put(c, "job"),
                        3 => cmd(c, Command::Use(t)),
                        4 => cmd(c, Command::Watch(t)),
                        5 => cmd(c, Command::ReserveWithTimeout(rnd(3) as u32)),
                        6 => cmd(c, Command::Delete(id)),
                        7 => cmd(
                            c,
                            Command::Release {
                                id,
                                pri: 1,
                                delay: rnd(2) as u32,
                            },
                        ),
                        8 => cmd(c, Command::Bury { id, pri: 1 }),
                        9 => cmd(c, Command::Kick(3)),
                        _ => cmd(c, Command::Touch(id)),
                    }
                };
                c_in(seq, input)
            }
            _ => Op::Tick,
        };
        // Track replies on the probe (skipping the deliberate duplicates).
        let t = stamp.max(last_now);
        last_now = t;
        let e = probe
            .get_or_insert_with(|| Engine::new(t, bstk_engine::EngineConfig::default(), sys()));
        let mut o = Outbox::new();
        if !dup {
            match &op {
                Op::Conn { input, .. } => {
                    if let EngineInput::Command { conn, .. } = input {
                        awaiting.insert(*conn);
                    }
                    e.apply_input(t, input.clone(), &mut o);
                }
                Op::Tick => e.apply_input(t, EngineInput::Tick, &mut o),
                Op::SetDraining(on) => e.apply_input(t, EngineInput::SetDraining(*on), &mut o),
                Op::DropNode { node, up_to_local } => {
                    let hit = |c: ConnId| owner_of(c) == *node && local(c) <= *up_to_local;
                    for c in e.conn_ids().into_iter().filter(|&c| hit(c)) {
                        awaiting.remove(&c);
                        e.apply_input(t, EngineInput::Disconnect(c), &mut o);
                    }
                    conns.retain(|&(c, _)| !hit(c));
                }
            }
        }
        for (c, _) in &o {
            awaiting.remove(c);
        }
        out.push(req(stamp, op));
    }
    out
}

/// Apply `reqs` in batches of the given sizes (cycled).
fn apply_batched(sm: &mut ClusterStateMachine, first: u64, reqs: &[Request], sizes: &[usize]) {
    let mut i = 0;
    let mut k = 0;
    while i < reqs.len() {
        let n = sizes[k % sizes.len()].min(reqs.len() - i);
        apply(sm, entries(first + i as u64, &reqs[i..i + n]));
        i += n;
        k += 1;
    }
}

fn deliveries_by_conn(evs: &[Ev]) -> BTreeMap<ConnId, Vec<Response>> {
    let mut m: BTreeMap<ConnId, Vec<Response>> = BTreeMap::new();
    for e in evs {
        if let Ev::Deliver(c, r) = e {
            m.entry(*c).or_default().push(r.clone());
        }
    }
    m
}

use std::collections::BTreeMap;

/// Every node ends in the same state; each node receives exactly the
/// replies for its own connections, as a single engine fed the applied
/// inputs would produce them, each once.
#[test]
fn nodes_fed_the_same_entries_agree() {
    for seed in [1u64, 7, 42, 1234] {
        let reqs = workload(600, seed);
        let mut ns: Vec<Node> = (1..=3).map(node).collect();
        let mut twin = node(1);
        for (i, n) in ns.iter_mut().enumerate() {
            apply_batched(&mut n.sm, 1, &reqs, &[1 + i, 7, 3]);
        }
        apply_batched(&mut twin.sm, 1, &reqs, &[50]);
        let s0 = state_bytes(&ns[0].sm);
        for n in &ns[1..] {
            assert_eq!(state_bytes(&n.sm), s0, "seed {seed}");
            assert_eq!(n.sm.handle().meta(), ns[0].sm.handle().meta());
        }
        assert_eq!(state_bytes(&twin.sm), s0);
        let evs: Vec<Vec<Ev>> = ns.iter().map(|n| n.sink.take()).collect();
        assert_eq!(twin.sink.take(), evs[0], "same node, different batching");

        // Reference: replay the inputs the state machine accepted on a
        // bare engine, with the same clamping.
        let mut probe = node(1);
        let mut expect: Outbox = Vec::new();
        let mut engine: Option<Engine> = None;
        let mut last_now = 0;
        for (i, r) in reqs.iter().enumerate() {
            let res = apply(
                &mut probe.sm,
                entries(1 + i as u64, std::slice::from_ref(r)),
            );
            let now = r.now.max(last_now);
            last_now = now;
            let e = engine.get_or_insert_with(|| {
                Engine::new(now, bstk_engine::EngineConfig::default(), sys())
            });
            if res[0].duplicate {
                continue;
            }
            match &r.op {
                Op::Conn { input, .. } => e.apply_input(now, input.clone(), &mut expect),
                Op::Tick => e.apply_input(now, EngineInput::Tick, &mut expect),
                Op::SetDraining(on) => {
                    e.apply_input(now, EngineInput::SetDraining(*on), &mut expect)
                }
                Op::DropNode { node, up_to_local } => {
                    for c in e
                        .conn_ids()
                        .into_iter()
                        .filter(|&c| owner_of(c) == *node && local(c) <= *up_to_local)
                    {
                        e.apply_input(now, EngineInput::Disconnect(c), &mut expect);
                    }
                }
            }
        }
        let engine = engine.unwrap();
        assert_eq!(postcard::to_allocvec(&engine.export_state()).unwrap(), s0);
        let mut total = 0;
        for (i, ev) in evs.iter().enumerate() {
            let node_id = i as u64 + 1;
            let got = deliveries_by_conn(ev);
            let mut want: BTreeMap<ConnId, Vec<Response>> = BTreeMap::new();
            for (c, r) in &expect {
                if owner_of(*c) == node_id {
                    want.entry(*c).or_default().push(r.clone());
                }
            }
            assert!(got.keys().all(|&c| owner_of(c) == node_id));
            assert_eq!(got, want, "seed {seed} node {node_id}");
            total += got.values().map(Vec::len).sum::<usize>();
        }
        assert_eq!(total, expect.len(), "every reply delivered once");
        assert!(total > 100, "workload too trivial: {total}");
    }
}

#[test]
fn already_applied_entries_are_skipped() {
    let mut n = node(1);
    let c = conn_id(1, 1);
    let reqs = [
        req(S, c_in(1, EngineInput::Connect(c))),
        req(S, c_in(2, cmd(c, Command::ListTubeUsed))),
    ];
    apply(&mut n.sm, entries(1, &reqs));
    assert_eq!(n.sink.take().len(), 3);
    let res = apply(&mut n.sm, entries(1, &reqs));
    assert_eq!(res.len(), 2);
    assert!(n.sink.take().is_empty());
}

#[test]
fn time_is_clamped_and_uptime_counts_from_the_first_entry() {
    let mut n = node(1);
    let h = n.sm.handle();
    let mut rx = h.subscribe();
    apply(&mut n.sm, vec![blank(1, 1)]);
    assert_eq!(h.last_now(), 0);
    apply(
        &mut n.sm,
        entries(2, &[req(5 * S, Op::Tick), req(3 * S, Op::Tick)]),
    );
    assert_eq!(h.last_now(), 5 * S);
    assert!(rx.has_changed().unwrap());
    assert_eq!(rx.borrow_and_update().last_applied, Some(lid(1, 3)));
    let snap = h.snapshot_limited(12 * S, 10).unwrap();
    assert_eq!(snap.server.uptime, 7);
    // A reserve-with-timeout sets a deadline that the leader must tick.
    let c = conn_id(1, 1);
    apply(
        &mut n.sm,
        entries(
            4,
            &[
                req(6 * S, c_in(1, EngineInput::Connect(c))),
                req(6 * S, c_in(2, cmd(c, Command::ReserveWithTimeout(2)))),
            ],
        ),
    );
    assert_eq!(h.next_deadline(), Some(8 * S));
    n.sink.take();
    apply(&mut n.sm, entries(6, &[req(8 * S, Op::Tick)]));
    assert_eq!(n.sink.take(), vec![Ev::Deliver(c, Response::TimedOut)]);
    assert_eq!(h.next_deadline(), None);
}

#[test]
fn blank_and_membership_entries_only_update_metadata() {
    let mut n = node(1);
    let before = state_bytes(&n.sm);
    let m = Membership::<NodeId, BasicNode>::new(vec![[1, 2, 3].into_iter().collect()], None);
    let ents = vec![
        blank(1, 1),
        Entry {
            log_id: lid(1, 2),
            payload: EntryPayload::Membership(m.clone()),
        },
    ];
    let res = apply(&mut n.sm, ents);
    assert_eq!(res, vec![Applied::default(); 2]);
    assert_eq!(state_bytes(&n.sm), before);
    let (last, mem) = block_on(n.sm.applied_state()).unwrap();
    assert_eq!(last, Some(lid(1, 2)));
    assert_eq!(mem, StoredMembership::new(Some(lid(1, 2)), m));
    assert!(n.sink.take().is_empty());
}

/// Build a snapshot at k, install it on a fresh node, continue: the result
/// equals never snapshotting. Also covers restarting from the snapshot.
#[test]
fn snapshot_install_continue_equals_uninterrupted() {
    let reqs = workload(500, 99);
    let k = 260;
    let mut a = node(2);
    apply_batched(&mut a.sm, 1, &reqs[..k], &[5]);
    let snap = block_on(a.sm.get_snapshot_builder())
        .build_snapshot()
        .pipe(block_on)
        .unwrap();
    assert_eq!(snap.meta.last_log_id, Some(lid(1, k as u64)));
    let cur = block_on(a.sm.get_current_snapshot()).unwrap().unwrap();
    assert_eq!(cur.meta, snap.meta);
    assert_eq!(cur.snapshot.get_ref(), snap.snapshot.get_ref());

    // Reference node that never snapshots.
    let mut c = node(2);
    apply_batched(&mut c.sm, 1, &reqs[..k], &[5]);
    let local_at_k: Vec<ConnId> =
        c.sm.handle()
            .conn_ids()
            .into_iter()
            .filter(|&x| owner_of(x) == 2)
            .collect();
    c.sink.take();

    // Fresh node installs.
    let mut b = node(2);
    let mut rx = block_on(b.sm.begin_receiving_snapshot()).unwrap();
    rx.get_mut().extend_from_slice(snap.snapshot.get_ref());
    block_on(b.sm.install_snapshot(&snap.meta, rx)).unwrap();
    assert_eq!(state_bytes(&b.sm), state_bytes(&a.sm));
    assert_eq!(
        block_on(b.sm.applied_state()).unwrap(),
        block_on(a.sm.applied_state()).unwrap()
    );
    // The skipped replies are reported as closed local connections.
    let closed: Vec<ConnId> = b
        .sink
        .take()
        .into_iter()
        .map(|e| match e {
            Ev::Closed(c) => c,
            other => panic!("unexpected {other:?}"),
        })
        .collect();
    assert_eq!(closed, local_at_k);
    let installed = block_on(b.sm.get_current_snapshot()).unwrap().unwrap();
    assert_eq!(installed.meta, snap.meta);

    // Continue all three.
    a.sink.take();
    for n in [&mut a, &mut b, &mut c] {
        apply_batched(&mut n.sm, k as u64 + 1, &reqs[k..], &[3]);
    }
    let sc = state_bytes(&c.sm);
    assert_eq!(state_bytes(&a.sm), sc);
    assert_eq!(state_bytes(&b.sm), sc);
    let ec = c.sink.take();
    assert!(!ec.is_empty());
    assert_eq!(a.sink.take(), ec);
    assert_eq!(b.sink.take(), ec);

    // Restart from the snapshot, then re-apply the log after it.
    let dir = a._dir.path().to_path_buf();
    let meta_a = a.sm.handle().meta();
    drop(a.sm);
    let sink = Arc::new(RecSink::default());
    let mut re = open_sm(&dir, 2, sink.clone());
    assert_eq!(
        block_on(re.applied_state()).unwrap().0,
        Some(lid(1, k as u64))
    );
    assert_eq!(state_bytes(&re), {
        let mut fresh = node(2);
        let mut rx = block_on(fresh.sm.begin_receiving_snapshot()).unwrap();
        rx.get_mut().extend_from_slice(snap.snapshot.get_ref());
        block_on(fresh.sm.install_snapshot(&snap.meta, rx)).unwrap();
        state_bytes(&fresh.sm)
    });
    // Entries at or below the snapshot are not re-applied.
    apply(&mut re, entries(1, &reqs[..k]));
    assert!(sink.take().is_empty());
    apply_batched(&mut re, k as u64 + 1, &reqs[k..], &[64]);
    assert_eq!(state_bytes(&re), sc);
    assert_eq!(re.handle().meta(), meta_a);
}

trait Pipe: Sized {
    fn pipe<R>(self, f: impl FnOnce(Self) -> R) -> R {
        f(self)
    }
}
impl<T> Pipe for T {}

fn build(
    sm: &mut ClusterStateMachine,
) -> Result<openraft::Snapshot<TypeConfig>, openraft::StorageError<NodeId>> {
    let mut b = block_on(sm.get_snapshot_builder());
    block_on(b.build_snapshot())
}

fn snap_files(dir: &Path) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|n| n != "lock")
        .collect();
    v.sort();
    v
}

#[test]
fn crash_mid_snapshot() {
    let reqs = workload(200, 5);
    let mut a = node(1);
    let dir = a._dir.path().to_path_buf();
    apply(&mut a.sm, entries(1, &reqs[..100]));
    let first = build(&mut a.sm).unwrap();
    apply(&mut a.sm, entries(101, &reqs[100..]));
    let at_200 = state_bytes(&a.sm);

    // Crash after writing the temporary file, before the rename.
    crash_point::arm(Some(Point::SnapshotBeforeRename));
    assert!(build(&mut a.sm).is_err());
    crash_point::arm(None);
    assert!(snap_files(&dir).iter().any(|n| n.ends_with(".tmp")));
    drop(a.sm);
    let sink = Arc::new(RecSink::default());
    let mut sm = open_sm(&dir, 1, sink.clone());
    assert_eq!(snap_files(&dir).len(), 1, "{:?}", snap_files(&dir));
    let cur = block_on(sm.get_current_snapshot()).unwrap().unwrap();
    assert_eq!(cur.meta, first.meta);
    // openraft re-applies the log after the snapshot.
    apply(&mut sm, entries(101, &reqs[100..]));
    assert_eq!(state_bytes(&sm), at_200);

    // Crash after the new snapshot is durable, before the old one is
    // removed: the newest wins, the old one is cleaned up.
    crash_point::arm(Some(Point::SnapshotBeforeCleanup));
    assert!(build(&mut sm).is_err());
    crash_point::arm(None);
    assert_eq!(snap_files(&dir).len(), 2);
    drop(sm);
    let mut sm = open_sm(&dir, 1, sink);
    assert_eq!(snap_files(&dir).len(), 1);
    assert_eq!(block_on(sm.applied_state()).unwrap().0, Some(lid(1, 200)));
    assert_eq!(state_bytes(&sm), at_200);

    // A normal build replaces the snapshot.
    let again = build(&mut sm).unwrap();
    assert_eq!(snap_files(&dir).len(), 1);
    assert_eq!(
        block_on(sm.get_current_snapshot()).unwrap().unwrap().meta,
        again.meta
    );
}

#[test]
fn damaged_snapshot_refuses_to_open() {
    let mut a = node(1);
    let dir = a._dir.path().to_path_buf();
    apply(&mut a.sm, entries(1, &workload(50, 3)));
    build(&mut a.sm).unwrap();
    drop(a.sm);
    let f = snap_files(&dir).pop().unwrap();
    let p = dir.join(f);
    let mut b = std::fs::read(&p).unwrap();
    let n = b.len();
    b[n / 2] ^= 1;
    std::fs::write(&p, &b).unwrap();
    assert!(matches!(
        ClusterStateMachine::open(&dir, sm_opts(1, Arc::new(RecSink::default()))),
        Err(crate::storage::OpenError::Corrupt(_))
    ));
}

#[test]
fn snapshot_dir_is_locked() {
    let a = node(1);
    assert!(matches!(
        ClusterStateMachine::open(a._dir.path(), sm_opts(1, Arc::new(RecSink::default()))),
        Err(crate::storage::OpenError::Locked(_))
    ));
}

/// Invalid snapshot data is rejected with a storage error; the state is
/// unchanged and nothing panics.
#[test]
fn install_rejects_invalid_snapshots() {
    let reqs = workload(300, 11);
    let mut a = node(1);
    apply(&mut a.sm, entries(1, &reqs));
    let snap = build(&mut a.sm).unwrap();
    let good = snap.snapshot.get_ref().clone();

    let mut b = node(1);
    apply(&mut b.sm, entries(1, &reqs[..10]));
    let before = state_bytes(&b.sm);
    let install = |b: &mut Node, bytes: Vec<u8>| {
        let mut rx = block_on(b.sm.begin_receiving_snapshot()).unwrap();
        *rx.get_mut() = bytes;
        block_on(b.sm.install_snapshot(&snap.meta, rx))
    };

    assert!(install(&mut b, b"garbage".to_vec()).is_err());
    assert!(install(&mut b, Vec::new()).is_err());
    assert!(install(&mut b, good[..good.len() / 2].to_vec()).is_err());

    // Metadata that disagrees with the engine.
    let mut p: SnapshotPayload = postcard::from_bytes(&good).unwrap();
    p.meta.next_seq.insert(conn_id(9, 9), 2);
    assert!(install(&mut b, postcard::to_allocvec(&p).unwrap()).is_err());
    let mut p: SnapshotPayload = postcard::from_bytes(&good).unwrap();
    p.meta.highest_local.clear();
    let has_conns = !p.meta.next_seq.is_empty();
    assert_eq!(
        install(&mut b, postcard::to_allocvec(&p).unwrap()).is_err(),
        has_conns
    );
    let mut p: SnapshotPayload = postcard::from_bytes(&good).unwrap();
    p.version = 99;
    assert!(install(&mut b, postcard::to_allocvec(&p).unwrap()).is_err());

    // Random byte flips: never a panic.
    let mut x = 0x9e37_79b9_7f4a_7c15u64;
    for _ in 0..300 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let mut bad = good.clone();
        let i = (x % bad.len() as u64) as usize;
        bad[i] ^= 1 << (x >> 61);
        let mut fresh = node(1);
        let _ = install(&mut fresh, bad);
    }
    assert_eq!(state_bytes(&b.sm), before);
    assert_eq!(block_on(b.sm.applied_state()).unwrap().0, Some(lid(1, 10)));
    assert!(install(&mut b, good).is_ok());
    assert_eq!(state_bytes(&b.sm), state_bytes(&a.sm));
}

/// The local number of a connection id.
fn local(c: ConnId) -> u64 {
    c & ((1 << crate::CONN_SEQ_BITS) - 1)
}

/// A bounded `DropNode` leaves the node's higher-numbered connections, and
/// their reservations, untouched.
#[test]
fn drop_node_with_a_bound_spares_higher_connections() {
    let old = conn_id(2, 3);
    let new = conn_id(2, 4);
    let reqs = [
        req(S, c_in(1, EngineInput::Connect(old))),
        req(S, c_in(1, EngineInput::Connect(new))),
        req(S, c_in(2, put(old, "a"))),
        req(S, c_in(3, put(old, "b"))),
        req(S, c_in(2, cmd(new, Command::Reserve))),
        req(
            2 * S,
            Op::DropNode {
                node: 2,
                up_to_local: 3,
            },
        ),
        // `new` is still open at its next seq.
        req(2 * S, c_in(3, cmd(new, Command::ListTubeUsed))),
        // `old` is gone.
        req(2 * S, c_in(4, cmd(old, Command::ListTubeUsed))),
    ];
    let mut n = node(2);
    let res = apply(&mut n.sm, entries(1, &reqs));
    assert_eq!(dups(&res)[5..], [false, false, true]);
    let h = n.sm.handle();
    assert_eq!(h.conn_ids(), vec![new]);
    assert_eq!(h.applied_seq(old), None);
    assert_eq!(h.applied_seq(new), Some(3));
    let closed: Vec<ConnId> = n
        .sink
        .take()
        .iter()
        .filter_map(|e| match e {
            Ev::Closed(c) => Some(*c),
            _ => None,
        })
        .collect();
    assert_eq!(closed, vec![old]);
    // The reservation of `new` is kept.
    let snap = h.snapshot_limited(2 * S, 10).unwrap();
    assert_eq!(snap.server.current_jobs_reserved, 1);
    assert_eq!(snap.server.current_connections, 1);
}

/// The race of a leader's `DropNode` for a node that restarted meanwhile:
/// the leader proposed it with the bound it saw (the old process's highest
/// local number), the node restarted, dropped its old connections itself
/// and accepted new ones above that number; the stale `DropNode` commits
/// only then and must not touch the new connections.
#[test]
fn stale_drop_node_after_restart_spares_new_connections() {
    let old = conn_id(2, 7);
    let other = conn_id(1, 1);
    let mut n = node(2);
    let before = [
        req(S, c_in(1, EngineInput::Connect(other))),
        req(S, c_in(1, EngineInput::Connect(old))),
        req(S, c_in(2, put(other, "job"))),
        req(S, c_in(2, cmd(old, Command::Reserve))),
    ];
    apply(&mut n.sm, entries(1, &before));
    let h = n.sm.handle();
    // The leader observes node 2 (silent) and prepares its DropNode.
    let leader_bound = h.highest_local(2);
    assert_eq!(leader_bound, 7);
    // Node 2 restarts: its own startup DropNode uses the bound it observed,
    // then it numbers new connections above it (as the server does).
    let own_bound = h.highest_local(2);
    let first_new = own_bound + 1;
    let new = conn_id(2, first_new);
    let after = [
        req(
            2 * S,
            Op::DropNode {
                node: 2,
                up_to_local: own_bound,
            },
        ),
        req(2 * S, c_in(1, EngineInput::Connect(new))),
        req(2 * S, c_in(2, cmd(new, Command::Reserve))),
        // The leader's stale DropNode commits last.
        req(
            3 * S,
            Op::DropNode {
                node: 2,
                up_to_local: leader_bound,
            },
        ),
        req(3 * S, c_in(3, cmd(new, Command::ListTubeUsed))),
    ];
    let res = apply(&mut n.sm, entries(1 + before.len() as u64, &after));
    assert_eq!(dups(&res), [false, false, false, false, false]);
    assert_eq!(h.conn_ids(), vec![other, new]);
    assert_eq!(h.applied_seq(new), Some(3));
    let snap = h.snapshot_limited(3 * S, 10).unwrap();
    // The job released from `old` is now reserved by `new`, and stays so.
    assert_eq!(snap.server.current_jobs_reserved, 1);
    let delivered: Vec<Ev> = n.sink.take();
    assert!(
        delivered
            .iter()
            .any(|e| matches!(e, Ev::Deliver(c, Response::Reserved { .. }) if *c == new)),
        "{delivered:?}"
    );
    assert!(
        !delivered
            .iter()
            .any(|e| matches!(e, Ev::Closed(c) if *c == new)),
        "{delivered:?}"
    );
}
