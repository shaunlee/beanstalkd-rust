//! Scale tests for the O(log n) buried/reservation removal (docs/PLAN.md
//! §7.2, §7.5; rationale in docs/DESIGN.md §4.6). Inserts happen in a
//! shuffled order that differs from job id order. Non-`#[ignore]`d tests run
//! at 100k; the `#[ignore]`d ones also time 1M and assert only an absolute
//! bound there: `cargo test --release -p bstk-engine --ignored -- --test-threads=1`.

#![allow(clippy::unwrap_used)]

use std::collections::HashSet;
use std::time::{Duration, Instant};

use bytes::Bytes;

use bstk_proto::{Command, JobId, Response, TubeName};

use crate::{
    ConnId, Engine, EngineConfig, JobRecord, Nanos, Outbox, RecordState, RecoveredJob, Recovery,
    StaticSysInfo,
};

fn tube(name: &str) -> TubeName {
    TubeName::new(name).expect("valid tube name")
}

fn engine(now: Nanos) -> Engine {
    Engine::new(
        now,
        EngineConfig::default(),
        Box::new(StaticSysInfo::default()),
    )
}

fn cmd(e: &mut Engine, now: Nanos, c: ConnId, cmd: Command) -> Outbox {
    let mut out = Outbox::new();
    e.handle(now, c, cmd, &mut out);
    out
}

fn only(out: &Outbox) -> &Response {
    assert_eq!(out.len(), 1, "expected exactly one reply, got {out:?}");
    &out[0].1
}

/// Dependency-free xorshift64 permutation of `1..=n`. Good enough to
/// decorrelate an order from job id order; not a real PRNG.
fn shuffled(n: u64, seed: u64) -> Vec<u64> {
    let mut v: Vec<u64> = (1..=n).collect();
    let mut x = seed | 1;
    for i in (1..v.len()).rev() {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        v.swap(i, (x % (i as u64 + 1)) as usize);
    }
    v
}

/// The smallest of `n` timings of `f`: jitter only adds delay, so the minimum
/// is the stablest estimate for runs this short.
fn min_of(n: usize, mut f: impl FnMut() -> Duration) -> Duration {
    (0..n).map(|_| f()).min().expect("n > 0")
}

/// Reports both timings and asserts only the absolute 1M bound (a ratio is
/// too noisy at 100k's scale; see docs/DESIGN.md §4.6).
fn assert_scales_to_1m(label: &str, small: Duration, big: Duration) {
    eprintln!("  {label}: 100k={small:?} 1M={big:?}");
    let bound = if cfg!(debug_assertions) {
        Duration::from_secs(60)
    } else {
        Duration::from_secs(15)
    };
    assert!(
        big < bound,
        "{label}: 1M took {big:?}, expected under {bound:?} (100k took {small:?})"
    );
}

/// Buries `order.len()` jobs, ids `1..=order.len()`, into tube "t", in
/// `order`, via `Engine::recover` -- this is O(n log n) setup that skips
/// put/bury command dispatch, since only the removal phase below is timed.
fn buried_setup(order: &[u64]) -> Engine {
    let jobs = order
        .iter()
        .map(|&id| RecoveredJob {
            record: JobRecord {
                id,
                pri: 0,
                delay: 0,
                ttr: 60,
                created_at: 0,
                deadline_at: 0,
                state: RecordState::Buried,
                reserve_ct: 0,
                timeout_ct: 0,
                release_ct: 0,
                bury_ct: 0,
                kick_ct: 0,
            },
            tube: tube("t"),
            body: Bytes::new(),
        })
        .collect();
    Engine::recover(
        0,
        EngineConfig::default(),
        Box::new(StaticSysInfo::default()),
        Recovery {
            jobs,
            next_id: order.len() as JobId + 1,
            tube_order: vec![tube("t")],
        },
    )
}

fn peek_buried(e: &mut Engine, c: ConnId) -> Option<JobId> {
    match only(&cmd(e, 0, c, Command::PeekBuried)) {
        Response::Found { id, .. } => Some(*id),
        Response::NotFound => None,
        other => panic!("unexpected reply to peek-buried: {other:?}"),
    }
}

fn run_buried_delete(n: u64, insert_seed: u64, removal_seed: u64) -> Duration {
    let insert_order = shuffled(n, insert_seed);
    let mut e = buried_setup(&insert_order);
    e.connect(0, 1);
    cmd(&mut e, 0, 1, Command::Use(tube("t")));

    let removal_order = shuffled(n, removal_seed);
    let mut removed: HashSet<JobId> = HashSet::with_capacity(n as usize);
    let mut front = 0usize;
    let start = Instant::now();
    for &id in &removal_order {
        let out = cmd(&mut e, 0, 1, Command::Delete(id));
        assert!(
            matches!(only(&out), Response::Deleted),
            "delete {id} failed: {out:?}"
        );
        removed.insert(id);
        while front < insert_order.len() && removed.contains(&insert_order[front]) {
            front += 1;
        }
        let expected = insert_order.get(front).copied();
        assert_eq!(
            peek_buried(&mut e, 1),
            expected,
            "buried FIFO diverged from the reference order after deleting {id}"
        );
    }
    let elapsed = start.elapsed();

    assert_eq!(e.t_tube_buried_len(&tube("t")), Some(0));
    e.validate().expect("invariants hold after scale deletion");
    elapsed
}

fn run_buried_kick_job(n: u64, insert_seed: u64, removal_seed: u64) -> Duration {
    let insert_order = shuffled(n, insert_seed);
    let mut e = buried_setup(&insert_order);
    e.connect(0, 1);
    cmd(&mut e, 0, 1, Command::Use(tube("t")));

    let removal_order = shuffled(n, removal_seed);
    let mut removed: HashSet<JobId> = HashSet::with_capacity(n as usize);
    let mut front = 0usize;
    let start = Instant::now();
    for &id in &removal_order {
        let out = cmd(&mut e, 0, 1, Command::KickJob(id));
        assert!(
            matches!(only(&out), Response::KickedJob),
            "kick-job {id} failed: {out:?}"
        );
        removed.insert(id);
        while front < insert_order.len() && removed.contains(&insert_order[front]) {
            front += 1;
        }
        let expected = insert_order.get(front).copied();
        assert_eq!(
            peek_buried(&mut e, 1),
            expected,
            "buried FIFO diverged from the reference order after kick-job {id}"
        );
    }
    let elapsed = start.elapsed();

    assert_eq!(e.t_tube_buried_len(&tube("t")), Some(0));
    assert_eq!(e.t_tube_ready_len(&tube("t")), Some(n as usize));
    e.validate().expect("invariants hold after scale kick-job");
    elapsed
}

/// Bulk `kick <n>` drains the buried FIFO from the front; checkpointed at
/// the midpoint and the end (not every job) so the check stays O(1) extra
/// per checkpoint instead of O(n).
fn run_buried_kick_bulk(n: u64, insert_seed: u64) -> Duration {
    let insert_order = shuffled(n, insert_seed);
    let mut e = buried_setup(&insert_order);
    e.connect(0, 1);
    cmd(&mut e, 0, 1, Command::Use(tube("t")));

    let half = n / 2;
    let start = Instant::now();
    let out = cmd(&mut e, 0, 1, Command::Kick(half as u32));
    assert_eq!(only(&out), &Response::Kicked(half));
    assert_eq!(
        peek_buried(&mut e, 1),
        insert_order.get(half as usize).copied(),
        "buried FIFO diverged from the reference order after bulk-kicking half"
    );
    let out = cmd(&mut e, 0, 1, Command::Kick(u32::MAX));
    assert_eq!(only(&out), &Response::Kicked(n - half));
    let elapsed = start.elapsed();

    assert_eq!(e.t_tube_buried_len(&tube("t")), Some(0));
    assert_eq!(e.t_tube_ready_len(&tube("t")), Some(n as usize));
    e.validate().expect("invariants hold after bulk kick");
    elapsed
}

#[test]
fn buried_delete_100k_random_order_is_fast_and_correct() {
    let elapsed = run_buried_delete(100_000, 1, 2);
    assert!(
        elapsed < Duration::from_secs(10),
        "took {elapsed:?} for 100k random-order deletes; a quadratic scan takes tens of \
         seconds here even in an unoptimized build"
    );
}

#[test]
fn buried_kick_job_100k_random_order_is_fast_and_correct() {
    let elapsed = run_buried_kick_job(100_000, 5, 6);
    assert!(
        elapsed < Duration::from_secs(10),
        "took {elapsed:?} for 100k random-order kick-job; looks quadratic"
    );
}

#[test]
fn buried_kick_bulk_100k_is_fast_and_correct() {
    let elapsed = run_buried_kick_bulk(100_000, 7);
    assert!(
        elapsed < Duration::from_secs(10),
        "took {elapsed:?} for a 100k bulk kick; looks quadratic"
    );
}

#[test]
#[ignore = "1M-job scale timing; run with `cargo test --release -p bstk-engine --ignored -- \
            --test-threads=1` (a few seconds per test in release, up to ~18s in debug)"]
fn buried_delete_time_scales_linearly_100k_vs_1m() {
    let small = min_of(3, || run_buried_delete(100_000, 1, 2));
    let big = run_buried_delete(1_000_000, 3, 4);
    assert_scales_to_1m("buried delete (random order)", small, big);
}

#[test]
#[ignore = "1M-job scale timing; run with `cargo test --release -p bstk-engine --ignored -- \
            --test-threads=1`"]
fn buried_kick_job_time_scales_linearly_100k_vs_1m() {
    let small = min_of(3, || run_buried_kick_job(100_000, 5, 6));
    let big = run_buried_kick_job(1_000_000, 7, 8);
    assert_scales_to_1m("buried kick-job (random order)", small, big);
}

#[test]
#[ignore = "1M-job scale timing; run with `cargo test --release -p bstk-engine --ignored -- \
            --test-threads=1`"]
fn buried_kick_bulk_time_scales_linearly_100k_vs_1m() {
    let small = min_of(3, || run_buried_kick_bulk(100_000, 9));
    let big = run_buried_kick_bulk(1_000_000, 10);
    assert_scales_to_1m("buried kick (bulk)", small, big);
}

/// Puts `n` jobs (sequential ids `1..=n`) and reserves them, on connection
/// 1, via `reserve-job` in `reserve_order` -- out of id order, so the
/// connection's reservation FIFO differs from id order.
fn reserved_setup(n: u64, reserve_order: &[u64]) -> Engine {
    let mut e = engine(0);
    e.connect(0, 1);
    for _ in 0..n {
        let out = cmd(
            &mut e,
            0,
            1,
            Command::Put {
                pri: 0,
                delay: 0,
                ttr: 60,
                body: Bytes::new(),
            },
        );
        assert!(matches!(only(&out), Response::Inserted(_)));
    }
    for &id in reserve_order {
        let out = cmd(&mut e, 0, 1, Command::ReserveJob(id));
        assert!(
            matches!(only(&out), Response::Reserved { .. }),
            "reserve-job {id} failed"
        );
    }
    e
}

/// Releases or deletes every reservation in `removal_order`, checking the
/// full remaining order (via `Engine::t_conn_reserved`, O(remaining)) at a
/// handful of checkpoints only -- no O(log n) "peek reservations" command
/// exists, so checking every step would make the test itself O(n^2).
fn remove_reserved_in_order(
    e: &mut Engine,
    reserve_order: &[u64],
    removal_order: &[u64],
    release: bool,
) -> Duration {
    let n = removal_order.len();
    let checkpoint_every = (n / 5).max(1);
    let mut removed: HashSet<JobId> = HashSet::with_capacity(n);

    let start = Instant::now();
    for (i, &id) in removal_order.iter().enumerate() {
        let out = if release {
            cmd(
                e,
                0,
                1,
                Command::Release {
                    id,
                    pri: 0,
                    delay: 0,
                },
            )
        } else {
            cmd(e, 0, 1, Command::Delete(id))
        };
        let ok = if release {
            matches!(only(&out), Response::Released)
        } else {
            matches!(only(&out), Response::Deleted)
        };
        assert!(ok, "removal of reservation {id} failed: {out:?}");
        removed.insert(id);
        if (i + 1) % checkpoint_every == 0 || i + 1 == n {
            let expected: Vec<JobId> = reserve_order
                .iter()
                .filter(|id| !removed.contains(id))
                .copied()
                .collect();
            assert_eq!(
                e.t_conn_reserved(1),
                expected,
                "reservation order diverged from the reference order after {} removals",
                i + 1
            );
        }
    }
    start.elapsed()
}

fn run_reserved_delete(n: u64, reserve_seed: u64, removal_seed: u64) -> Duration {
    let reserve_order = shuffled(n, reserve_seed);
    let mut e = reserved_setup(n, &reserve_order);
    let removal_order = shuffled(n, removal_seed);
    let elapsed = remove_reserved_in_order(&mut e, &reserve_order, &removal_order, false);
    assert!(e.t_conn_reserved(1).is_empty());
    e.validate().expect("invariants hold after scale deletion");
    elapsed
}

fn run_reserved_release(n: u64, reserve_seed: u64, removal_seed: u64) -> Duration {
    let reserve_order = shuffled(n, reserve_seed);
    let mut e = reserved_setup(n, &reserve_order);
    let removal_order = shuffled(n, removal_seed);
    let elapsed = remove_reserved_in_order(&mut e, &reserve_order, &removal_order, true);
    assert!(e.t_conn_reserved(1).is_empty());
    e.validate().expect("invariants hold after scale release");
    elapsed
}

#[test]
fn reserved_delete_100k_random_order_is_fast_and_correct() {
    let elapsed = run_reserved_delete(100_000, 11, 12);
    assert!(
        elapsed < Duration::from_secs(10),
        "took {elapsed:?} for 100k random-order reservation deletes; looks quadratic"
    );
}

#[test]
#[ignore = "1M-job scale timing; run with `cargo test --release -p bstk-engine --ignored -- \
            --test-threads=1`"]
fn reserved_release_time_scales_linearly_100k_vs_1m() {
    let small = min_of(3, || run_reserved_release(100_000, 13, 14));
    let big = run_reserved_release(1_000_000, 15, 16);
    assert_scales_to_1m(
        "reservation release (random order, one connection)",
        small,
        big,
    );
}

#[test]
#[ignore = "1M-job scale timing; run with `cargo test --release -p bstk-engine --ignored -- \
            --test-threads=1`"]
fn reserved_delete_time_scales_linearly_100k_vs_1m() {
    let small = min_of(3, || run_reserved_delete(100_000, 11, 12));
    let big = run_reserved_delete(1_000_000, 17, 18);
    assert_scales_to_1m(
        "reservation delete (random order, one connection)",
        small,
        big,
    );
}
