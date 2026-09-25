//! Write / fsync ordering (docs/PLAN.md §4.4, "an instrumented file layer
//! proves the order write → fsync → reply" for `-f0`).
//!
//! `Inner::file_ops` records every segment write and fsync in order. With
//! `SyncPolicy::Always`, every `append` must write all of its records and
//! then fsync each segment it wrote before returning; the engine actor
//! releases replies only after `append` returns (proved by
//! `engine_actor::tests::replies_are_released_only_after_append_returns`
//! in bstk-server). Together: write → fsync → reply for every
//! acknowledged change.

use super::*;
use crate::format::{DELETE_REC_LEN, UPDATE_REC_LEN, put_rec_len};
use crate::wal::FileOp;

fn rec_len(e: &JournalEntry) -> u64 {
    match e {
        JournalEntry::Put { tube, body, .. } => put_rec_len(tube.as_str().len(), body.len()),
        JournalEntry::Update(_) => UPDATE_REC_LEN,
        JournalEntry::Delete(_) => DELETE_REC_LEN,
    }
}

/// A deterministic mix of batches: single puts, bury updates, deletes,
/// multi-entry batches (like `kick 10`) and puts large enough to roll over
/// to a new segment in the middle of a batch.
fn batches() -> Vec<Vec<JournalEntry>> {
    let mut out = Vec::new();
    let mut live: Vec<JobId> = Vec::new();
    let mut next = 1;
    for step in 0u64..400 {
        let mut b = Vec::new();
        match step % 7 {
            0..=2 => {
                let size = [10usize, 300, 2500][(step % 3) as usize];
                b.push(put(next, "tube", &vec![b'x'; size]));
                live.push(next);
                next += 1;
            }
            3 if !live.is_empty() => {
                b.push(buried(live[live.len() / 2], 1));
            }
            4 => {
                // 12 x 700 bytes do not fit in one 8 KiB segment.
                for _ in 0..(if step % 2 == 0 { 12 } else { 5 }) {
                    b.push(put(next, "t", &vec![b'y'; 700]));
                    live.push(next);
                    next += 1;
                }
            }
            5 if live.len() > 3 => {
                for id in live.drain(..3) {
                    b.push(JournalEntry::Delete(id));
                }
            }
            _ => {
                for &id in live.iter().take(4) {
                    b.push(buried(id, 2));
                }
            }
        }
        if !b.is_empty() {
            out.push(b);
        }
    }
    out
}

#[test]
fn always_policy_fsyncs_every_write_before_append_returns() {
    let t = tmp();
    let mut o = opts(t.path(), 8192);
    o.sync = SyncPolicy::Always;
    let (mut wal, _) = Wal::open(o).unwrap();
    let mut rollovers = 0;
    for (n, b) in batches().iter().enumerate() {
        wal.inner.file_ops.clear();
        write(&mut wal, b);
        let ops = std::mem::take(&mut wal.inner.file_ops);

        // Every record reached the file during this call.
        let written: u64 = ops
            .iter()
            .map(|op| match op {
                FileOp::Write { len, .. } => *len,
                _ => 0,
            })
            .sum();
        let expected: u64 = b.iter().map(rec_len).sum();
        assert_eq!(written, expected, "batch {n}: bytes written");

        // Every write is followed by an fsync of its segment, and the call
        // ends with an fsync (nothing written after the last one).
        for (k, op) in ops.iter().enumerate() {
            if let FileOp::Write { seg, .. } = op {
                assert!(
                    ops[k + 1..]
                        .iter()
                        .any(|o| matches!(o, FileOp::Sync { seg: s } if s == seg)),
                    "batch {n}: write to binlog.{seg} not fsynced before append returned: {ops:?}"
                );
            }
        }
        assert!(
            matches!(ops.last(), Some(FileOp::Sync { .. })),
            "batch {n}: append did not end with an fsync: {ops:?}"
        );
        let segs: std::collections::BTreeSet<u64> = ops
            .iter()
            .filter_map(|op| match op {
                FileOp::Write { seg, .. } => Some(*seg),
                _ => None,
            })
            .collect();
        if segs.len() > 1 {
            rollovers += 1;
        }

        // Compaction and GC run after the replies' records are durable;
        // GC fsyncs before it unlinks anything.
        wal.maintain().unwrap();
        wal.inner.check_invariants();
    }
    assert!(rollovers > 0, "the batches must cross segment boundaries");
}

#[test]
fn other_policies_never_fsync_inside_append() {
    for policy in [
        SyncPolicy::Never,
        SyncPolicy::Interval(std::time::Duration::from_secs(3600)),
    ] {
        let t = tmp();
        let mut o = opts(t.path(), 8192);
        o.sync = policy;
        let (mut wal, _) = Wal::open(o).unwrap();
        for (n, b) in batches().iter().enumerate() {
            wal.inner.file_ops.clear();
            write(&mut wal, b);
            let ops = std::mem::take(&mut wal.inner.file_ops);
            let written: u64 = ops
                .iter()
                .map(|op| match op {
                    FileOp::Write { len, .. } => *len,
                    _ => 0,
                })
                .sum();
            let expected: u64 = b.iter().map(rec_len).sum();
            assert_eq!(written, expected, "{policy:?} batch {n}: bytes written");
            // A rollover fsyncs the finished segment unless `Never`; the
            // new tail is not synced.
            if policy == SyncPolicy::Never {
                assert!(
                    ops.iter().all(|o| matches!(o, FileOp::Write { .. })),
                    "{policy:?} batch {n}: {ops:?}"
                );
            } else {
                assert!(
                    matches!(ops.last(), Some(FileOp::Write { .. })),
                    "{policy:?} batch {n}: {ops:?}"
                );
            }
        }
    }
}
