use std::time::{Duration, Instant};

use bstk_engine::JournalEntry;

use super::*;
use crate::WalError;
use crate::format::{self, DELETE_REC_LEN, Parsed, Rec, UPDATE_REC_LEN, parse_at, put_rec_len};

#[test]
fn format_roundtrip() {
    let mut buf = Vec::new();
    let r = rec(7);
    format::encode_put(&mut buf, &r, &tube("t-1"), b"hello").unwrap();
    assert_eq!(buf.len() as u64, put_rec_len(3, 5));
    let p = buf.len();
    format::encode_update(&mut buf, &r).unwrap();
    assert_eq!((buf.len() - p) as u64, UPDATE_REC_LEN);
    let d = buf.len();
    format::encode_delete(&mut buf, 7).unwrap();
    assert_eq!((buf.len() - d) as u64, DELETE_REC_LEN);

    match parse_at(&buf, 0) {
        Parsed::Rec {
            rec:
                Rec::Put {
                    record,
                    tube: t,
                    body,
                },
            len,
        } => {
            assert_eq!(record, r);
            assert_eq!(t.as_str(), "t-1");
            assert_eq!(body, b"hello");
            assert_eq!(len as usize, p);
        }
        other => panic!("{other:?}"),
    }
    assert!(matches!(parse_at(&buf, p), Parsed::Rec { rec: Rec::Update(x), .. } if x == r));
    assert!(matches!(
        parse_at(&buf, d),
        Parsed::Rec {
            rec: Rec::Delete(7),
            ..
        }
    ));
    assert!(matches!(parse_at(&buf, buf.len()), Parsed::End));

    // Restamping keeps a valid CRC and changes the record.
    let mut put = buf[..p].to_vec();
    let mut r2 = r.clone();
    r2.bury_ct = 9;
    format::restamp_put(&mut put, &r2);
    assert!(
        matches!(parse_at(&put, 0), Parsed::Rec { rec: Rec::Put { record, .. }, .. } if record == r2)
    );

    // Any flipped bit is detected.
    for i in 0..p {
        let mut bad = buf[..p].to_vec();
        bad[i] ^= 0x10;
        assert!(!matches!(parse_at(&bad, 0), Parsed::Rec { .. }), "byte {i}");
    }
}

#[test]
fn open_creates_dir_lock_and_first_segment() {
    let t = tmp();
    let dir = t.path().join("a/b");
    let (wal, r) = Wal::open(opts(&dir, 1000)).unwrap();
    assert_eq!(
        r,
        Recovery {
            jobs: vec![],
            next_id: 1,
            tube_order: vec![],
        }
    );
    assert!(dir.join("lock").exists());
    let files = seg_files(&dir);
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].0, 1);
    assert_eq!(std::fs::metadata(&files[0].1).unwrap().len(), 4096);
    let s = wal.stats();
    assert_eq!((s.oldest_index, s.current_index), (1, 1));
    assert_eq!((s.records_written, s.records_migrated), (0, 0));
}

#[test]
fn file_size_rounds_up_to_4096() {
    let t = tmp();
    let (_wal, _) = Wal::open(opts(t.path(), 5000)).unwrap();
    let files = seg_files(t.path());
    assert_eq!(std::fs::metadata(&files[0].1).unwrap().len(), 8192);
}

#[test]
fn lock_contention() {
    let t = tmp();
    let (wal, _) = Wal::open(opts(t.path(), 4096)).unwrap();
    assert!(matches!(
        Wal::open(opts(t.path(), 4096)),
        Err(WalError::Locked)
    ));
    drop(wal);
    let (_wal, _) = Wal::open(opts(t.path(), 4096)).unwrap();
}

#[test]
fn put_update_delete_recover() {
    let t = tmp();
    let (mut wal, _) = Wal::open(opts(t.path(), 4096)).unwrap();
    write(&mut wal, &[put(3, "b", b"three"), put(1, "a", b"one")]);
    write(&mut wal, &[put(2, "a", b"two"), buried(1, 1)]);
    write(&mut wal, &[JournalEntry::Delete(2)]);
    assert_eq!(wal.stats().records_written, 5);
    wal.inner.check_invariants();
    drop(wal);

    let (wal, r) = Wal::open(opts(t.path(), 4096)).unwrap();
    wal.inner.check_invariants();
    let ids: Vec<_> = r.jobs.iter().map(|j| j.record.id).collect();
    assert_eq!(ids, vec![3, 1]);
    assert_eq!(r.jobs[0].tube.as_str(), "b");
    assert_eq!(&r.jobs[0].body[..], b"three");
    assert_eq!(r.jobs[1].record.state, bstk_engine::RecordState::Buried);
    assert_eq!(r.jobs[1].record.bury_ct, 1);
    assert_eq!(r.next_id, 4);
    // A new current segment was started (binlog.2 was an unused spare
    // and has been removed; numbers are never reused).
    assert_eq!(wal.stats().current_index, 3);
    assert_eq!(wal.stats().oldest_index, 1);
    assert_eq!(wal.stats().records_written, 0);
}

#[test]
fn tube_order_follows_reference_replay() {
    let t = tmp();
    let (mut wal, _) = Wal::open(opts(t.path(), 4096)).unwrap();
    write(
        &mut wal,
        &[put(1, "x", b""), put(2, "default", b""), put(3, "y", b"")],
    );
    write(
        &mut wal,
        &[put(4, "z", b""), put(5, "w", b""), put(6, "x", b"")],
    );
    // x still has job 6: no change. Then y's last job goes: swap-remove.
    write(
        &mut wal,
        &[JournalEntry::Delete(1), JournalEntry::Delete(3)],
    );
    drop(wal);
    let (_wal, r) = Wal::open(opts(t.path(), 4096)).unwrap();
    let names: Vec<_> = r.tube_order.iter().map(|t| t.as_str()).collect();
    // [x, y, z, w] -> remove y -> [x, w, z]
    assert_eq!(names, vec!["x", "w", "z"]);
}

#[test]
fn tube_order_swap_removes_and_reappends() {
    let t = tmp();
    let (mut wal, _) = Wal::open(opts(t.path(), 4096)).unwrap();
    let names = ["t1", "t2", "t3", "t4", "t5"];
    for (i, n) in names.iter().enumerate() {
        write(&mut wal, &[put(i as u64 + 1, n, b"body")]);
    }
    // Updates and deletes of unknown jobs don't touch the list.
    write(
        &mut wal,
        &[buried(2, 1), JournalEntry::Delete(99), buried(98, 1)],
    );
    write(&mut wal, &[JournalEntry::Delete(3)]);
    drop(wal);
    let (mut wal, r) = Wal::open(opts(t.path(), 4096)).unwrap();
    let got: Vec<_> = r.tube_order.iter().map(|t| t.as_str()).collect();
    assert_eq!(got, vec!["t1", "t2", "t5", "t4"]);
    write(&mut wal, &[put(10, "t3", b"again")]);
    drop(wal);
    let (_wal, r) = Wal::open(opts(t.path(), 4096)).unwrap();
    let got: Vec<_> = r.tube_order.iter().map(|t| t.as_str()).collect();
    assert_eq!(got, vec!["t1", "t2", "t5", "t4", "t3"]);
}

#[test]
fn next_id_counts_deleted_and_ignored_records() {
    let t = tmp();
    let (mut wal, _) = Wal::open(opts(t.path(), 4096)).unwrap();
    write(&mut wal, &[put(1, "a", b"x"), put(5, "a", b"y")]);
    write(&mut wal, &[JournalEntry::Delete(5)]);
    drop(wal);
    let (mut wal, r) = Wal::open(opts(t.path(), 4096)).unwrap();
    assert_eq!(r.next_id, 6);
    assert_eq!(r.jobs.len(), 1);
    // An update or delete for a job without a surviving put is ignored but
    // still counts toward next_id.
    wal.append(&[buried(40, 1), JournalEntry::Delete(41)])
        .unwrap();
    drop(wal);
    let (_wal, r) = Wal::open(opts(t.path(), 4096)).unwrap();
    assert_eq!(r.next_id, 42);
    assert_eq!(r.jobs.len(), 1);
}

#[test]
fn new_segment_every_open_and_indexes_never_reused() {
    let t = tmp();
    let mut seen = Vec::new();
    for i in 0..5u64 {
        let (mut wal, _) = Wal::open(opts(t.path(), 4096)).unwrap();
        let cur = wal.stats().current_index;
        assert!(seen.iter().all(|&s| s < cur), "{seen:?} {cur}");
        seen.push(cur);
        write(&mut wal, &[put(i + 1, "a", b"x")]);
        write(&mut wal, &[JournalEntry::Delete(i + 1)]);
        wal.maintain().unwrap();
    }
    // Dead segments were collected.
    assert!(seg_files(t.path()).len() <= 3);
}

#[test]
fn rollover_across_many_segments() {
    let t = tmp();
    let (mut wal, _) = Wal::open(opts(t.path(), 4096)).unwrap();
    let body = [7u8; 100];
    for id in 1..=1000 {
        write(
            &mut wal,
            &[put(id, if id % 2 == 0 { "even" } else { "odd" }, &body)],
        );
    }
    for id in (1..=1000).filter(|i| i % 3 == 0) {
        write(&mut wal, &[buried(id, 1)]);
    }
    wal.inner.check_invariants();
    let s = wal.stats();
    assert!(s.current_index > 30, "{s:?}");
    drop(wal);
    let (wal, r) = Wal::open(opts(t.path(), 4096)).unwrap();
    wal.inner.check_invariants();
    assert_eq!(r.jobs.len(), 1000);
    for (i, j) in r.jobs.iter().enumerate() {
        let id = i as u64 + 1;
        assert_eq!(j.record.id, id);
        assert_eq!(j.record.bury_ct, u32::from(id.is_multiple_of(3)));
        assert_eq!(&j.body[..], &body[..]);
    }
    assert_eq!(r.next_id, 1001);
}

#[test]
fn sync_policies() {
    let t = tmp();
    let mut o = opts(t.path(), 4096);
    o.sync = SyncPolicy::Always;
    let (mut wal, _) = Wal::open(o).unwrap();
    let before = wal.inner.sync_count;
    write(&mut wal, &[put(1, "a", b"x")]);
    assert_eq!(wal.inner.sync_count, before + 1);
    wal.sync_if_due(Instant::now()).unwrap();
    assert_eq!(wal.inner.sync_count, before + 1);
    drop(wal);

    let t = tmp();
    let mut o = opts(t.path(), 4096);
    o.sync = SyncPolicy::Interval(Duration::from_millis(50));
    let (mut wal, _) = Wal::open(o).unwrap();
    let now = Instant::now();
    wal.sync_if_due(now).unwrap();
    assert_eq!(wal.inner.sync_count, 0, "nothing to sync");
    write(&mut wal, &[put(1, "a", b"x")]);
    assert_eq!(wal.inner.sync_count, 0, "append does not sync");
    wal.sync_if_due(now).unwrap();
    assert_eq!(wal.inner.sync_count, 1, "first sync is due at once");
    write(&mut wal, &[put(2, "a", b"x")]);
    wal.sync_if_due(now + Duration::from_millis(10)).unwrap();
    assert_eq!(wal.inner.sync_count, 1, "interval not elapsed");
    wal.sync_if_due(now + Duration::from_millis(50)).unwrap();
    assert_eq!(wal.inner.sync_count, 2);
    wal.sync_if_due(now + Duration::from_millis(200)).unwrap();
    assert_eq!(wal.inner.sync_count, 2, "nothing unsynced");
    drop(wal);

    let t = tmp();
    let (mut wal, _) = Wal::open(opts(t.path(), 4096)).unwrap();
    for id in 1..200 {
        write(&mut wal, &[put(id, "a", &[1; 64])]);
    }
    wal.sync_if_due(Instant::now()).unwrap();
    wal.maintain().unwrap();
    assert_eq!(wal.inner.sync_count, 0);
}

#[test]
fn reservation_fails_at_limit_but_updates_and_deletes_proceed() {
    let t = tmp();
    // Room for 3 segments of 4096 bytes: the current one and two spares.
    let (mut wal, _) = Wal::open_with_limit(opts(t.path(), 4096), 3 * 4096).unwrap();
    let body = [0u8; 200];
    let mut id = 0;
    while wal.reserve_put(1, body.len()) {
        id += 1;
        wal.append(&[put(id, "a", &body)]).unwrap();
        assert!(id < 1000);
    }
    assert!(id > 5, "{id}");
    assert_eq!(seg_files(t.path()).len(), 3);
    // Still refused: nothing changed.
    assert!(!wal.reserve_put(1, body.len()));
    // Updates use the spare.
    for j in 1..=id {
        wal.append(&[buried(j, 1)]).unwrap();
    }
    // Deletes never need allocation and free space after compaction/gc.
    for j in 1..=id {
        wal.append(&[JournalEntry::Delete(j)]).unwrap();
    }
    wal.maintain().unwrap();
    wal.inner.check_invariants();
    assert!(wal.reserve_put(1, body.len()));
}

#[test]
fn updates_fail_only_once_the_spare_is_exhausted() {
    let t = tmp();
    let (mut wal, _) = Wal::open_with_limit(opts(t.path(), 4096), 3 * 4096).unwrap();
    let mut id = 0;
    while wal.reserve_put(1, 200) {
        id += 1;
        wal.append(&[put(id, "a", &[0; 200])]).unwrap();
    }
    let mut n: u64 = 0;
    loop {
        match wal.append(&[buried(n % id + 1, 1)]) {
            Ok(()) => n += 1,
            Err(WalError::Io(e)) => {
                assert_eq!(e.kind(), std::io::ErrorKind::StorageFull);
                break;
            }
            Err(e) => panic!("{e}"),
        }
        assert!(n < 10_000);
    }
    // At least a whole segment's worth of updates fit.
    assert!(n * UPDATE_REC_LEN >= 4096 - 16, "{n}");
}

#[test]
fn unconsumed_put_reservations_are_released_by_next_append() {
    let t = tmp();
    let (mut wal, _) = Wal::open_with_limit(opts(t.path(), 4096), 2 * 4096).unwrap();
    let mut n = 0;
    while wal.reserve_put(1, 100) {
        n += 1;
        assert!(n < 100);
    }
    assert!(n > 0);
    // Those puts were never journaled (e.g. rejected by the engine).
    wal.append(&[]).unwrap();
    assert!(wal.reserve_put(1, 100));
}

#[test]
fn oversized_put() {
    let t = tmp();
    let (mut wal, _) = Wal::open(opts(t.path(), 4096)).unwrap();
    let big = vec![9u8; 10_000];
    assert!(!wal.reserve_put(1, big.len()));
    // Appended anyway (unreserved): it gets a segment of its own.
    wal.append(&[
        put(1, "a", b"small"),
        put(2, "a", &big),
        put(3, "a", b"after"),
    ])
    .unwrap();
    wal.inner.check_invariants();
    drop(wal);
    let (wal, r) = Wal::open(opts(t.path(), 4096)).unwrap();
    wal.inner.check_invariants();
    assert_eq!(r.jobs.len(), 3);
    assert_eq!(r.jobs[1].body.len(), 10_000);
}

#[test]
fn stats_count_moves() {
    let t = tmp();
    let (mut wal, _) = Wal::open(opts(t.path(), 4096)).unwrap();
    write(&mut wal, &[put(1, "keep", &[1; 50])]);
    let mut id = 2;
    let mut written = 1;
    while wal.stats().records_migrated == 0 {
        write(&mut wal, &[put(id, "tmp", &[2; 300])]);
        write(&mut wal, &[JournalEntry::Delete(id)]);
        written += 2;
        id += 1;
        wal.maintain().unwrap();
        wal.inner.check_invariants();
        assert!(id < 10_000);
    }
    let s = wal.stats();
    assert_eq!(s.records_written, written + s.records_migrated);
    assert!(s.oldest_index > 1, "binlog.1 was collected: {s:?}");
}

#[test]
fn unknown_files_are_ignored_and_bad_headers_rejected() {
    let t = tmp();
    for name in ["binlog.01", "binlog.x", "binlog.", "binlog.2.tmp", "other"] {
        std::fs::write(t.path().join(name), b"garbage").unwrap();
    }
    let (wal, r) = Wal::open(opts(t.path(), 4096)).unwrap();
    assert!(r.jobs.is_empty());
    assert_eq!(wal.stats().current_index, 1);
    drop(wal);

    let t = tmp();
    std::fs::write(seg_path(t.path(), 1), [b'X'; 64]).unwrap();
    assert!(matches!(
        Wal::open(opts(t.path(), 4096)),
        Err(WalError::Corrupt(_))
    ));

    let t = tmp();
    let mut h = format::segment_header().to_vec();
    h[8] = 99;
    std::fs::write(seg_path(t.path(), 1), &h).unwrap();
    assert!(matches!(
        Wal::open(opts(t.path(), 4096)),
        Err(WalError::Corrupt(_))
    ));
}

#[test]
fn incomplete_segments_are_removed() {
    let t = tmp();
    let (mut wal, _) = Wal::open(opts(t.path(), 4096)).unwrap();
    write(&mut wal, &[put(1, "a", b"x")]);
    drop(wal);
    // Interrupted creations: empty, short header, zero header.
    std::fs::write(seg_path(t.path(), 5), b"").unwrap();
    std::fs::write(seg_path(t.path(), 6), b"BSTK").unwrap();
    std::fs::write(seg_path(t.path(), 7), [0u8; 4096]).unwrap();
    let (wal, r) = Wal::open(opts(t.path(), 4096)).unwrap();
    assert_eq!(r.jobs.len(), 1);
    assert_eq!(wal.stats().current_index, 8);
    let idx: Vec<_> = seg_files(t.path()).iter().map(|x| x.0).collect();
    assert!(
        !idx.contains(&5) && !idx.contains(&6) && !idx.contains(&7),
        "{idx:?}"
    );
}

/// A wrapped `-s -1` (or anything past the cap) is rejected before any
/// file is created, instead of overflowing or preallocating a huge segment.
#[test]
fn absurd_segment_size_is_rejected() {
    let t = tmp();
    let dir = t.path().join("wal");
    for size in [u64::MAX, u64::MAX - 4095, (1 << 32) + 1] {
        match Wal::open(opts(&dir, size)) {
            Err(WalError::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput),
            other => panic!("expected InvalidInput for {size}, got {other:?}"),
        }
    }
    assert!(!dir.exists(), "no directory should be created");
}
