use std::fs::OpenOptions;
use std::os::unix::fs::FileExt;
use std::path::Path;

use bstk_engine::JournalEntry;

use super::*;
use crate::WalError;

fn zero_from(path: &Path, from: u64, to: u64) {
    let f = OpenOptions::new().write(true).open(path).unwrap();
    if to > from {
        f.write_all_at(&vec![0u8; (to - from) as usize], from)
            .unwrap();
    }
}

fn flip(path: &Path, at: u64) {
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let mut b = [0u8; 1];
    f.read_exact_at(&mut b, at).unwrap();
    b[0] ^= 0x5a;
    f.write_all_at(&b, at).unwrap();
}

/// Write a history, then a final batch one entry at a time, recording the
/// end offset of each final record. Returns (dir, model before the final
/// batch, final entries, segment of the final batch, start, record ends).
fn setup_tail() -> (
    tempfile::TempDir,
    Model,
    Vec<JournalEntry>,
    u64,
    u64,
    Vec<u64>,
) {
    let t = tmp();
    let (mut wal, _) = Wal::open(opts(t.path(), 4096)).unwrap();
    let mut model = Model::default();
    let hist = [
        put(1, "a", b"first"),
        put(2, "b", b"second"),
        buried(1, 1),
        put(3, "a", b"third"),
        JournalEntry::Delete(2),
    ];
    for e in &hist {
        write(&mut wal, std::slice::from_ref(e));
        model.apply(e);
    }
    let fin = vec![
        put(4, "c", b"fourth job body"),
        buried(3, 1),
        JournalEntry::Delete(1),
        put(5, "a", b""),
    ];
    let (seg, start) = wal.inner.cur_pos();
    let mut ends = Vec::new();
    for e in &fin {
        write(&mut wal, std::slice::from_ref(e));
        let (s, end) = wal.inner.cur_pos();
        assert_eq!(s, seg);
        ends.push(end);
    }
    wal.inner.check_invariants();
    drop(wal);
    (t, model, fin, seg, start, ends)
}

#[test]
fn truncation_at_every_offset_of_the_final_records() {
    let (t, base, fin, seg, start, ends) = setup_tail();
    let end = *ends.last().unwrap();
    // A spare segment exists after the tail segment, so the tail is not in
    // the last file.
    assert!(seg_files(t.path()).last().unwrap().0 > seg);
    let original = std::fs::read(seg_path(t.path(), seg)).unwrap();
    let starts: Vec<u64> = std::iter::once(start).chain(ends.iter().copied()).collect();
    for cut in start..=end {
        for zero_fill in [false, true] {
            let d = copy_dir(t.path());
            let p = seg_path(d.path(), seg);
            if zero_fill {
                zero_from(&p, cut, end);
            } else {
                OpenOptions::new()
                    .write(true)
                    .open(&p)
                    .unwrap()
                    .set_len(cut)
                    .unwrap();
            }
            // A record survives if it ends before the cut or, when
            // zero-filling, if the zeroed part of it was already zero.
            let intact = |i: usize| {
                let (s, e) = (starts[i], ends[i]);
                e <= cut
                    || (zero_fill
                        && original[s.max(cut) as usize..e as usize]
                            .iter()
                            .all(|&b| b == 0))
            };
            let complete = (0..ends.len()).take_while(|&i| intact(i)).count();
            let mut m = base.clone();
            for e in &fin[..complete] {
                m.apply(e);
            }
            let (wal, r) = Wal::open(opts(d.path(), 4096)).unwrap();
            wal.inner.check_invariants();
            assert_eq!(r.jobs, m.expected_jobs(), "cut {cut} zero {zero_fill}");
            assert_eq!(r.next_id, m.max_id + 1, "cut {cut}");
            drop(wal);
            // The torn tail was truncated: reopening is still fine.
            let (_wal, r2) = Wal::open(opts(d.path(), 4096)).unwrap();
            assert_eq!(r2.jobs, r.jobs);
        }
    }
}

#[test]
fn crc_error_in_last_record_is_a_torn_tail() {
    let (t, base, fin, seg, _start, ends) = setup_tail();
    let last_start = ends[ends.len() - 2];
    for at in last_start..ends[ends.len() - 1] {
        let d = copy_dir(t.path());
        flip(&seg_path(d.path(), seg), at);
        let mut m = base.clone();
        for e in &fin[..fin.len() - 1] {
            m.apply(e);
        }
        let (_wal, r) = Wal::open(opts(d.path(), 4096)).unwrap();
        assert_eq!(r.jobs, m.expected_jobs(), "flip at {at}");
    }
}

#[test]
fn crc_error_mid_last_segment_truncates_there() {
    // A failed record in the last segment with data ends replay even when
    // valid records follow it: everything from there on is discarded.
    let (t, base, fin, seg, start, ends) = setup_tail();
    let starts: Vec<u64> = std::iter::once(start).chain(ends.iter().copied()).collect();
    for (k, at) in [(0, start + 12), (2, ends[1] + 10), (1, ends[0] + 2)] {
        assert!(starts[k] <= at && at < ends[k]);
        let d = copy_dir(t.path());
        flip(&seg_path(d.path(), seg), at);
        let mut m = base.clone();
        for e in &fin[..k] {
            m.apply(e);
        }
        let (wal, r) = Wal::open(opts(d.path(), 4096)).unwrap();
        wal.inner.check_invariants();
        assert_eq!(r.jobs, m.expected_jobs(), "flip at {at}");
        assert_eq!(
            std::fs::metadata(seg_path(d.path(), seg)).unwrap().len(),
            starts[k],
            "truncated at the bad record"
        );
        drop(wal);
        let (_wal, r2) = Wal::open(opts(d.path(), 4096)).unwrap();
        assert_eq!(r2.jobs, r.jobs);
    }
}

#[test]
fn crc_error_in_earlier_segment_is_corrupt() {
    // In an earlier segment: any byte of any record.
    let t = tmp();
    let (mut wal, _) = Wal::open(opts(t.path(), 4096)).unwrap();
    for id in 1..=100 {
        write(&mut wal, &[put(id, "a", &[3; 100])]);
    }
    drop(wal);
    let files = seg_files(t.path());
    assert!(files.len() > 3);
    let first = &files[0].1;
    let len = std::fs::metadata(first).unwrap().len();
    for at in (16..len).step_by(37).chain([len - 1]) {
        let d = copy_dir(t.path());
        flip(&seg_path(d.path(), files[0].0), at);
        assert!(
            matches!(Wal::open(opts(d.path(), 4096)), Err(WalError::Corrupt(_))),
            "flip at {at}"
        );
    }
    // Truncating an earlier segment in the middle of a record, too.
    let d = copy_dir(t.path());
    OpenOptions::new()
        .write(true)
        .open(seg_path(d.path(), files[0].0))
        .unwrap()
        .set_len(len - 3)
        .unwrap();
    assert!(matches!(
        Wal::open(opts(d.path(), 4096)),
        Err(WalError::Corrupt(_))
    ));
    // Garbage after the end marker of an earlier segment.
    let d = copy_dir(t.path());
    let p = seg_path(d.path(), files[0].0);
    let f = OpenOptions::new().write(true).open(&p).unwrap();
    f.write_all_at(&[0u8; 8], len).unwrap();
    f.write_all_at(&[1u8; 8], len + 8).unwrap();
    assert!(matches!(
        Wal::open(opts(d.path(), 4096)),
        Err(WalError::Corrupt(_))
    ));
    // Unmodified copy is fine.
    let d = copy_dir(t.path());
    let (_wal, r) = Wal::open(opts(d.path(), 4096)).unwrap();
    assert_eq!(r.jobs.len(), 100);
}

/// Drive churn with long-lived jobs until a `maintain` call both moves a
/// job and deletes a segment. Returns the directory copies from right
/// before that call, the model, and the dir after it.
struct MidCompaction {
    before: tempfile::TempDir,
    model: Model,
    /// Deleted segments.
    deleted: Vec<u64>,
    /// Current segment before and after, with offsets.
    pos_before: (u64, u64),
    pos_after: (u64, u64),
    after: tempfile::TempDir,
}

fn mid_compaction() -> MidCompaction {
    let t = tmp();
    let (mut wal, _) = Wal::open(opts(t.path(), 4096)).unwrap();
    let mut model = Model::default();
    let long: Vec<_> = (1..=4)
        .map(|id| put(id, "long", format!("long-lived {id}").as_bytes()))
        .collect();
    for e in &long {
        write(&mut wal, std::slice::from_ref(e));
        model.apply(e);
    }
    let b = buried(2, 1);
    write(&mut wal, std::slice::from_ref(&b));
    model.apply(&b);
    let mut id = 100;
    loop {
        let e = [put(id, "tmp", &[5; 250]), JournalEntry::Delete(id)];
        write(&mut wal, &e);
        for x in &e {
            model.apply(x);
        }
        id += 1;
        assert!(id < 100_000);
        let before_files = seg_files(t.path());
        let migrated = wal.stats().records_migrated;
        let pos_before = wal.inner.cur_pos();
        let before = copy_dir(t.path());
        wal.maintain().unwrap();
        wal.inner.check_invariants();
        let after_files = seg_files(t.path());
        let deleted: Vec<u64> = before_files
            .iter()
            .map(|x| x.0)
            .filter(|i| !after_files.iter().any(|a| a.0 == *i))
            .collect();
        if wal.stats().records_migrated > migrated && !deleted.is_empty() {
            let pos_after = wal.inner.cur_pos();
            drop(wal);
            return MidCompaction {
                before,
                model,
                deleted,
                pos_before,
                pos_after,
                after: t,
            };
        }
    }
}

#[test]
fn crash_mid_compaction_same_live_set() {
    let mc = mid_compaction();
    assert!(mc.model.jobs.len() == 4);

    // (a) The moves were written but the old segments were not deleted:
    // the moved jobs are in two files; the last copy wins.
    let d = copy_dir(mc.after.path());
    for i in &mc.deleted {
        std::fs::copy(seg_path(mc.before.path(), *i), seg_path(d.path(), *i)).unwrap();
    }
    let (wal, r) = Wal::open(opts(d.path(), 4096)).unwrap();
    wal.inner.check_invariants();
    assert!(mc.model.same_set(&r), "{r:?}");
    drop(wal);

    // (b) Crash before anything of the maintain call reached the disk.
    let (_wal, r) = Wal::open(opts(mc.before.path(), 4096)).unwrap();
    assert!(mc.model.same_set(&r));

    // (c) Torn in the middle of the moves (old segments still present).
    let (seg, end) = mc.pos_after;
    let start = if mc.pos_before.0 == seg {
        mc.pos_before.1
    } else {
        crate::format::SEG_HEADER_LEN
    };
    assert!(end > start);
    for cut in start..=end {
        let d = copy_dir(mc.after.path());
        for i in &mc.deleted {
            std::fs::copy(seg_path(mc.before.path(), *i), seg_path(d.path(), *i)).unwrap();
        }
        zero_from(&seg_path(d.path(), seg), cut, end);
        let (wal, r) = Wal::open(opts(d.path(), 4096)).unwrap();
        wal.inner.check_invariants();
        assert!(mc.model.same_set(&r), "cut {cut}: {r:?}");
    }
}

#[test]
fn deleted_job_never_resurrected() {
    let t = tmp();
    let (mut wal, _) = Wal::open(opts(t.path(), 4096)).unwrap();
    let mut model = Model::default();
    // Job 1 shares its segment with long-lived job 2, so that segment
    // survives while job 1's delete is in a later one.
    for e in [put(1, "a", b"victim"), put(2, "a", b"keeper")] {
        write(&mut wal, std::slice::from_ref(&e));
        model.apply(&e);
    }
    let mut deleted = false;
    for (id, step) in (10u64..).zip(0..3000) {
        let mut e = vec![put(id, "t", &[1; 120])];
        if step % 2 == 1 {
            e.push(JournalEntry::Delete(id - 1));
        }
        if step == 100 {
            e.push(JournalEntry::Delete(1));
            deleted = true;
        }
        write(&mut wal, &e);
        for x in &e {
            model.apply(x);
        }
        if step % 7 == 0 {
            wal.maintain().unwrap();
        }
        if step % 97 == 0 || step == 101 {
            let d = copy_dir(t.path());
            let (_w, r) = Wal::open(opts(d.path(), 4096)).unwrap();
            assert!(model.same_set(&r), "step {step}");
            assert_eq!(deleted, !r.jobs.iter().any(|j| j.record.id == 1));
        }
    }
    drop(wal);
    for _ in 0..3 {
        let (mut wal, r) = Wal::open(opts(t.path(), 4096)).unwrap();
        assert!(model.same_set(&r));
        assert!(!r.jobs.iter().any(|j| j.record.id == 1));
        wal.maintain().unwrap();
    }
}

#[test]
fn moved_job_replays_at_its_new_position() {
    let t = tmp();
    let (mut wal, _) = Wal::open(opts(t.path(), 4096)).unwrap();
    write(&mut wal, &[put(1, "first", b"one")]);
    // Fill a few segments, then put job 2.
    let mut id = 10;
    while wal.stats().current_index < 4 {
        write(&mut wal, &[put(id, "t", &[0; 200])]);
        write(&mut wal, &[JournalEntry::Delete(id)]);
        id += 1;
    }
    write(&mut wal, &[put(2, "second", b"two")]);
    // Before any compaction the order is the put order.
    let (_w, r) = Wal::open(opts(copy_dir(t.path()).path(), 4096)).unwrap();
    let ids: Vec<_> = r.jobs.iter().map(|j| j.record.id).collect();
    assert_eq!(ids, vec![1, 2]);
    // Churn until job 1 has been moved out of binlog.1 and binlog.1 is gone.
    while wal.stats().oldest_index == 1 {
        write(&mut wal, &[put(id, "t", &[0; 200])]);
        write(&mut wal, &[JournalEntry::Delete(id)]);
        id += 1;
        wal.maintain().unwrap();
        assert!(id < 100_000);
    }
    assert!(wal.inner.moved.contains(&1));
    assert!(
        !wal.inner.moved.contains(&2),
        "job 2 is not in the oldest segment"
    );
    drop(wal);
    // Job 1's first surviving record is now its move, after job 2's put.
    let (_wal, r) = Wal::open(opts(t.path(), 4096)).unwrap();
    let ids: Vec<_> = r.jobs.iter().map(|j| j.record.id).collect();
    assert_eq!(ids, vec![2, 1]);
    assert_eq!(r.jobs[1].tube.as_str(), "first");
    assert_eq!(&r.jobs[1].body[..], b"one");
}

#[test]
fn churn_keeps_disk_usage_bounded() {
    let t = tmp();
    let fs = 64 * 1024;
    let (mut wal, _) = Wal::open(opts(t.path(), fs)).unwrap();
    let mut model = Model::default();
    for id in 1..=20 {
        let e = put(id, "long", &[id as u8; 100]);
        write(&mut wal, std::slice::from_ref(&e));
        model.apply(&e);
    }
    let mut max_segs = 0;
    let mut max_bytes = 0;
    let mut live_tmp = std::collections::VecDeque::new();
    for i in 0..100_000u64 {
        let id = 1000 + i;
        let e = put(id, "tmp", &[7; 64]);
        write(&mut wal, std::slice::from_ref(&e));
        model.apply(&e);
        live_tmp.push_back(id);
        if live_tmp.len() > 50 {
            let d = JournalEntry::Delete(live_tmp.pop_front().unwrap());
            wal.append(std::slice::from_ref(&d)).unwrap();
            model.apply(&d);
        }
        if i % 1000 == 0 {
            // Some updates of long-lived jobs.
            let u = buried((i / 1000) % 20 + 1, (i / 1000) as u32);
            wal.append(std::slice::from_ref(&u)).unwrap();
            model.apply(&u);
        }
        wal.maintain().unwrap();
        if i % 500 == 0 {
            let files = seg_files(t.path());
            max_segs = max_segs.max(files.len());
            let bytes: u64 = files
                .iter()
                .map(|f| std::fs::metadata(&f.1).unwrap().len())
                .sum();
            max_bytes = max_bytes.max(bytes);
        }
    }
    wal.inner.check_invariants();
    assert!(max_segs <= 6, "max segments {max_segs}");
    assert!(max_bytes <= 6 * fs, "max bytes {max_bytes}");
    assert!(wal.stats().current_index > 100);
    drop(wal);
    let (_wal, r) = Wal::open(opts(t.path(), fs)).unwrap();
    assert!(model.same_set(&r));
}
