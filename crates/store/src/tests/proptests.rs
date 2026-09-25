//! Random journal histories with random compaction and crash points,
//! compared against an in-memory model after every reopen.

use std::collections::HashSet;
use std::fs::OpenOptions;
use std::os::unix::fs::FileExt;

use bstk_engine::{JournalEntry, RecordState};
use bytes::Bytes;
use proptest::prelude::*;

use super::*;

#[derive(Debug, Clone)]
enum EOp {
    Put { tube: u8, body_len: u16, fill: u8 },
    Update { pick: usize, state: u8, n: u32 },
    Delete { pick: usize },
}

#[derive(Debug, Clone)]
enum Op {
    Batch(Vec<EOp>),
    Maintain,
    Sync,
    /// Drop the Wal and reopen. With `tear`, the last append (if it was
    /// the last write) is torn at the given fraction of its bytes.
    Crash {
        tear: Option<(u16, bool)>,
    },
}

fn eop() -> impl Strategy<Value = EOp> {
    prop_oneof![
        4 => (0u8..4, 0u16..400, any::<u8>())
            .prop_map(|(tube, body_len, fill)| EOp::Put { tube, body_len, fill }),
        2 => (any::<usize>(), 0u8..3, any::<u32>())
            .prop_map(|(pick, state, n)| EOp::Update { pick, state, n }),
        3 => any::<usize>().prop_map(|pick| EOp::Delete { pick }),
    ]
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        10 => prop::collection::vec(eop(), 1..5).prop_map(Op::Batch),
        3 => Just(Op::Maintain),
        1 => Just(Op::Sync),
        2 => prop::option::of((any::<u16>(), any::<bool>()))
            .prop_map(|tear| Op::Crash { tear }),
    ]
}

const TUBES: [&str; 4] = ["default", "a", "tube-b", "c_c"];

struct LastBatch {
    before: Model,
    entries: Vec<JournalEntry>,
    start: (u64, u64),
    end: (u64, u64),
}

/// Compare a recovery with the model. Jobs that compaction has ever moved
/// may appear at a later position (their first surviving record can be a
/// move); every other job must keep the model's relative order.
fn check(
    model: &Model,
    r: &Recovery,
    moved: &HashSet<u64>,
    exact_tubes: bool,
) -> Result<(), TestCaseError> {
    prop_assert!(
        model.same_set(r),
        "live set differs: {:?} vs {:?}",
        r.jobs,
        model.jobs
    );
    let got: Vec<u64> = r
        .jobs
        .iter()
        .map(|j| j.record.id)
        .filter(|id| !moved.contains(id))
        .collect();
    let want: Vec<u64> = model
        .order
        .iter()
        .copied()
        .filter(|id| !moved.contains(id))
        .collect();
    prop_assert_eq!(got, want);
    // The tube list is exact while no segment with records has been
    // deleted (binlog.1 still exists); otherwise it must at least list
    // each live non-default tube once.
    if exact_tubes {
        prop_assert_eq!(&r.tube_order, &model.tubes);
    } else {
        let mut want: Vec<&str> = r
            .jobs
            .iter()
            .map(|j| j.tube.as_str())
            .filter(|t| *t != "default")
            .collect();
        want.sort_unstable();
        want.dedup();
        let mut got: Vec<&str> = r.tube_order.iter().map(|t| t.as_str()).collect();
        got.sort_unstable();
        prop_assert_eq!(got, want);
    }
    let max_live = r.jobs.iter().map(|j| j.record.id).max().unwrap_or(0);
    prop_assert!(r.next_id > max_live);
    prop_assert!(r.next_id >= 1 && r.next_id <= model.max_id + 1);
    Ok(())
}

fn run(file_size: u64, policy: u8, ops: Vec<Op>) -> Result<(), TestCaseError> {
    let t = tmp();
    let mut o = opts(t.path(), file_size);
    o.sync = match policy {
        0 => SyncPolicy::Interval(std::time::Duration::from_millis(1)),
        _ => SyncPolicy::Never,
    };
    let (mut wal, _) = Wal::open(o.clone()).unwrap();
    let mut model = Model::default();
    let mut next_id: u64 = 1;
    let mut moved: HashSet<u64> = HashSet::new();
    let mut exact_tubes = true;
    let mut last: Option<LastBatch> = None;
    let ops = ops.into_iter().chain([Op::Crash { tear: None }]);

    for op in ops {
        match op {
            Op::Batch(eops) => {
                let mut scratch = model.clone();
                let mut entries = Vec::new();
                for e in eops {
                    let live: Vec<u64> = scratch.order.clone();
                    let entry = match e {
                        EOp::Put {
                            tube,
                            body_len,
                            fill,
                        } => {
                            let id = next_id;
                            next_id += 1;
                            let mut r = rec(id);
                            r.pri = u32::from(fill);
                            JournalEntry::Put {
                                record: r,
                                tube: super::tube(TUBES[tube as usize]),
                                body: Bytes::from(vec![fill; body_len as usize]),
                            }
                        }
                        EOp::Update { pick, state, n } => {
                            if live.is_empty() {
                                continue;
                            }
                            let id = live[pick % live.len()];
                            let mut r = scratch.jobs[&id].record.clone();
                            r.state = [
                                RecordState::Ready,
                                RecordState::Delayed,
                                RecordState::Buried,
                            ][state as usize];
                            r.deadline_at = u64::from(n);
                            r.bury_ct = n;
                            r.kick_ct = r.kick_ct.wrapping_add(1);
                            JournalEntry::Update(r)
                        }
                        EOp::Delete { pick } => {
                            if live.is_empty() {
                                continue;
                            }
                            JournalEntry::Delete(live[pick % live.len()])
                        }
                    };
                    scratch.apply(&entry);
                    entries.push(entry);
                }
                if entries.is_empty() {
                    continue;
                }
                let start = wal.inner.cur_pos();
                write(&mut wal, &entries);
                let end = wal.inner.cur_pos();
                last = Some(LastBatch {
                    before: model.clone(),
                    entries,
                    start,
                    end,
                });
                model = scratch;
            }
            Op::Maintain => {
                wal.maintain().unwrap();
                last = None;
            }
            Op::Sync => {
                wal.sync_if_due(std::time::Instant::now()).unwrap();
            }
            Op::Crash { tear } => {
                moved.extend(wal.inner.moved.iter().copied());
                let dir = wal.inner.dir().to_path_buf();
                drop(wal);
                let torn = match (tear, &last) {
                    (Some((frac, zero_fill)), Some(lb)) => {
                        let (seg, end) = lb.end;
                        let start = if lb.start.0 == seg { lb.start.1 } else { 16 };
                        let cut = start + (end - start) * u64::from(frac) / u64::from(u16::MAX);
                        let p = seg_path(&dir, seg);
                        let f = OpenOptions::new().write(true).open(&p).unwrap();
                        if zero_fill {
                            if end > cut {
                                f.write_all_at(&vec![0u8; (end - cut) as usize], cut)
                                    .unwrap();
                            }
                        } else {
                            f.set_len(cut).unwrap();
                        }
                        true
                    }
                    _ => false,
                };
                let (w, r) = Wal::open(o.clone()).unwrap();
                if !seg_path(&dir, 1).exists() {
                    exact_tubes = false;
                }
                w.inner.check_invariants();
                if torn {
                    let lb = last.as_ref().unwrap();
                    // Some prefix of the torn batch survived.
                    let mut m = lb.before.clone();
                    let mut ok = check(&m, &r, &moved, exact_tubes).is_ok();
                    for e in &lb.entries {
                        if ok {
                            break;
                        }
                        m.apply(e);
                        ok = check(&m, &r, &moved, exact_tubes).is_ok();
                    }
                    prop_assert!(ok, "no prefix of the torn batch matches: {:?}", r.jobs);
                    model = m;
                } else {
                    check(&model, &r, &moved, exact_tubes)?;
                }
                let max_id = model.max_id;
                model = Model::from_recovery(&r);
                model.max_id = model.max_id.max(max_id);
                next_id = next_id.max(r.next_id);
                wal = w;
                last = None;
            }
        }
        wal.inner.check_invariants();
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 1000, ..ProptestConfig::default() })]

    #[test]
    fn random_histories_recover_like_the_model(
        file_size in prop_oneof![Just(4096u64), Just(8192u64)],
        policy in 0u8..4,
        ops in prop::collection::vec(op(), 1..60),
    ) {
        run(file_size, policy, ops)?;
    }
}
