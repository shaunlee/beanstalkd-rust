//! Log store: persistence, torn tails, corruption, vote, truncate, purge.

use std::path::{Path, PathBuf};

use openraft::storage::{RaftLogStorage, RaftLogStorageExt};
use openraft::{CommittedLeaderId, LogId, RaftLogReader, Vote};

use super::*;
use crate::storage::OpenError;

fn append(log: &mut LogStore, ents: impl IntoIterator<Item = Entry<TypeConfig>>) {
    let ents: Vec<_> = ents.into_iter().collect();
    block_on(log.blocking_append(ents)).unwrap();
}

fn read_all(log: &mut LogStore) -> Vec<Entry<TypeConfig>> {
    block_on(log.try_get_log_entries(..)).unwrap()
}

fn state(log: &mut LogStore) -> (Option<u64>, Option<u64>) {
    let s = block_on(log.get_log_state()).unwrap();
    (
        s.last_purged_log_id.map(|l| l.index),
        s.last_log_id.map(|l| l.index),
    )
}

fn indexes(ents: &[Entry<TypeConfig>]) -> Vec<u64> {
    ents.iter().map(|e| e.log_id.index).collect()
}

fn segments(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "seg"))
        .collect();
    v.sort();
    v
}

fn copy_dir(from: &Path, to: &Path) {
    for e in std::fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        std::fs::copy(e.path(), to.join(e.file_name())).unwrap();
    }
}

#[test]
fn append_read_across_segments_and_reopen() {
    let d = tempfile::tempdir().unwrap();
    let mut log = open_log(d.path());
    assert_eq!(state(&mut log), (None, None));
    // Several batches, one fdatasync each.
    append(&mut log, (1..=5).map(|i| filler(1, i)));
    append(&mut log, (6..=40).map(|i| filler(1, i)));
    append(&mut log, [filler(2, 41)]);
    let all = read_all(&mut log);
    assert_eq!(indexes(&all), (1..=41).collect::<Vec<_>>());
    assert_eq!(all[40], filler(2, 41));
    assert!(segments(d.path()).len() > 3, "small segments must roll");
    let m = log.metrics();
    assert_eq!((m.first_index, m.last_index), (Some(1), Some(41)));
    drop(log);

    let mut log = open_log(d.path());
    assert_eq!(state(&mut log), (None, Some(41)));
    let again = read_all(&mut log);
    assert_eq!(again, all);
    let part = block_on(log.try_get_log_entries(10..13)).unwrap();
    assert_eq!(indexes(&part), vec![10, 11, 12]);
    let limited = block_on(log.limited_get_log_entries(1, 42)).unwrap();
    assert_eq!(limited.len(), 41);
    append(&mut log, [filler(2, 42)]);
    assert_eq!(state(&mut log), (None, Some(42)));
}

#[test]
fn limited_reads_are_bounded_but_nonempty() {
    let d = tempfile::tempdir().unwrap();
    let mut log = LogStore::open(
        d.path(),
        LogOptions {
            segment_size: 1 << 20,
            max_read_bytes: 100,
        },
    )
    .unwrap();
    append(&mut log, (1..=30).map(|i| filler(1, i)));
    let got = block_on(log.limited_get_log_entries(1, 31)).unwrap();
    assert!(!got.is_empty() && got.len() < 30, "got {}", got.len());
    assert_eq!(got[0].log_id.index, 1);
    let one = block_on(log.limited_get_log_entries(30, 31)).unwrap();
    assert_eq!(indexes(&one), vec![30]);
}

#[test]
fn non_consecutive_append_is_rejected() {
    let d = tempfile::tempdir().unwrap();
    let mut log = open_log(d.path());
    append(&mut log, [filler(1, 1), filler(1, 2)]);
    assert!(block_on(log.blocking_append([filler(1, 4)])).is_err());
    assert!(block_on(log.blocking_append([filler(1, 2)])).is_err());
    append(&mut log, [filler(1, 3)]);
    assert_eq!(state(&mut log), (None, Some(3)));
}

/// Cut the last segment at every byte offset (a torn append) and reopen:
/// the complete records survive, the rest is dropped, and the log keeps
/// working.
#[test]
fn torn_tail_at_every_offset() {
    let base = tempfile::tempdir().unwrap();
    {
        let mut log = open_log(base.path());
        append(&mut log, (1..=14).map(|i| filler(1, i)));
    }
    let segs = segments(base.path());
    let last = segs.last().unwrap().clone();
    let last_name = last.file_name().unwrap().to_owned();
    let full = std::fs::read(&last).unwrap();
    // Record boundaries in the last segment, from a clean read.
    let first_in_last: u64 = last_name.to_str().unwrap()[..20].parse().unwrap();
    assert!(first_in_last > 1, "need more than one segment");
    let mut ends = vec![24u64];
    {
        let mut off = 24usize;
        while off < full.len() {
            let len = u32::from_le_bytes(full[off..off + 4].try_into().unwrap()) as usize;
            off += 8 + len;
            ends.push(off as u64);
        }
    }
    for cut in 0..full.len() as u64 {
        let d = tempfile::tempdir().unwrap();
        copy_dir(base.path(), d.path());
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(d.path().join(&last_name))
            .unwrap();
        f.set_len(cut).unwrap();
        drop(f);

        let mut log = open_log(d.path());
        let complete = ends.iter().filter(|&&e| e <= cut).count() as u64;
        // `complete` counts the header end too; records = complete - 1.
        let records = complete.saturating_sub(1);
        let want_last = first_in_last - 1 + records;
        assert_eq!(state(&mut log), (None, Some(want_last)), "cut at {cut}");
        let all = read_all(&mut log);
        assert_eq!(indexes(&all), (1..=want_last).collect::<Vec<_>>());
        append(&mut log, [filler(2, want_last + 1)]);
        drop(log);
        let mut log = open_log(d.path());
        assert_eq!(state(&mut log), (None, Some(want_last + 1)), "cut at {cut}");
        assert_eq!(
            read_all(&mut log).last().unwrap(),
            &filler(2, want_last + 1)
        );
    }
}

/// Garbage after the last complete record (an append whose bytes landed
/// out of order) is cut off.
#[test]
fn garbage_tail_is_cut() {
    let d = tempfile::tempdir().unwrap();
    {
        let mut log = open_log(d.path());
        append(&mut log, (1..=3).map(|i| filler(1, i)));
    }
    let last = segments(d.path()).pop().unwrap();
    let mut bytes = std::fs::read(&last).unwrap();
    let good_len = bytes.len();
    bytes.extend_from_slice(&[0x10, 0, 0, 0, 1, 2, 3, 4]);
    bytes.extend_from_slice(&[0xab; 16]);
    std::fs::write(&last, &bytes).unwrap();
    let mut log = open_log(d.path());
    assert_eq!(state(&mut log), (None, Some(3)));
    assert_eq!(std::fs::metadata(&last).unwrap().len(), good_len as u64);
    // A flipped byte inside the last record of the last segment.
    drop(log);
    let mut bytes = std::fs::read(&last).unwrap();
    let n = bytes.len();
    bytes[n - 1] ^= 0xff;
    std::fs::write(&last, &bytes).unwrap();
    let mut log = open_log(d.path());
    let (_, l) = state(&mut log);
    assert!(l.unwrap() < 3);
}

#[test]
fn corruption_before_the_tail_refuses_to_open() {
    let base = tempfile::tempdir().unwrap();
    {
        let mut log = open_log(base.path());
        append(&mut log, (1..=20).map(|i| filler(1, i)));
        block_on(log.save_vote(&Vote::new(3, 1))).unwrap();
    }
    let segs = segments(base.path());
    assert!(segs.len() >= 3);
    let name = |p: &PathBuf| p.file_name().unwrap().to_owned();

    // A flipped payload byte in the first segment.
    let d = tempfile::tempdir().unwrap();
    copy_dir(base.path(), d.path());
    let p = d.path().join(name(&segs[0]));
    let mut b = std::fs::read(&p).unwrap();
    b[24 + 10] ^= 1;
    std::fs::write(&p, &b).unwrap();
    assert!(matches!(
        LogStore::open(d.path(), small_log_opts()),
        Err(OpenError::Corrupt(_))
    ));

    // A truncated middle segment.
    let d = tempfile::tempdir().unwrap();
    copy_dir(base.path(), d.path());
    let p = d.path().join(name(&segs[1]));
    let len = std::fs::metadata(&p).unwrap().len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&p)
        .unwrap()
        .set_len(len - 3)
        .unwrap();
    assert!(matches!(
        LogStore::open(d.path(), small_log_opts()),
        Err(OpenError::Corrupt(_))
    ));

    // A missing middle segment (a gap).
    let d = tempfile::tempdir().unwrap();
    copy_dir(base.path(), d.path());
    std::fs::remove_file(d.path().join(name(&segs[1]))).unwrap();
    assert!(matches!(
        LogStore::open(d.path(), small_log_opts()),
        Err(OpenError::Corrupt(_))
    ));

    // A bad segment header in a non-empty last segment.
    let d = tempfile::tempdir().unwrap();
    copy_dir(base.path(), d.path());
    let p = d.path().join(name(segs.last().unwrap()));
    let mut b = std::fs::read(&p).unwrap();
    b[0] ^= 1;
    std::fs::write(&p, &b).unwrap();
    assert!(matches!(
        LogStore::open(d.path(), small_log_opts()),
        Err(OpenError::Corrupt(_))
    ));

    // A damaged vote file.
    let d = tempfile::tempdir().unwrap();
    copy_dir(base.path(), d.path());
    let p = d.path().join("vote");
    let mut b = std::fs::read(&p).unwrap();
    let n = b.len();
    b[n - 1] ^= 1;
    std::fs::write(&p, &b).unwrap();
    assert!(matches!(
        LogStore::open(d.path(), small_log_opts()),
        Err(OpenError::Corrupt(_))
    ));

    // The untouched copy opens.
    let d = tempfile::tempdir().unwrap();
    copy_dir(base.path(), d.path());
    let mut log = open_log(d.path());
    assert_eq!(state(&mut log), (None, Some(20)));
}

/// A crash right after a new segment file was created: an empty or partial
/// header in the last segment is removed.
#[test]
fn crash_after_segment_creation() {
    for header_bytes in [0usize, 5, 24] {
        let d = tempfile::tempdir().unwrap();
        {
            let mut log = open_log(d.path());
            append(&mut log, (1..=3).map(|i| filler(1, i)));
        }
        let mut hdr = Vec::new();
        hdr.extend_from_slice(b"BSTKRLOG");
        hdr.extend_from_slice(&1u32.to_le_bytes());
        hdr.extend_from_slice(&0u32.to_le_bytes());
        hdr.extend_from_slice(&4u64.to_le_bytes());
        hdr.truncate(header_bytes);
        let p = d.path().join(format!("{:020}.seg", 4));
        std::fs::write(&p, &hdr).unwrap();
        let mut log = open_log(d.path());
        assert_eq!(state(&mut log), (None, Some(3)));
        assert!(!p.exists());
        append(&mut log, [filler(1, 4)]);
        assert_eq!(indexes(&read_all(&mut log)), vec![1, 2, 3, 4]);
    }
}

#[test]
fn lock_prevents_a_second_open() {
    let d = tempfile::tempdir().unwrap();
    let log = open_log(d.path());
    assert!(matches!(
        LogStore::open(d.path(), small_log_opts()),
        Err(OpenError::Locked(_))
    ));
    drop(log);
    open_log(d.path());
}

#[test]
fn vote_is_durable_and_atomic() {
    let d = tempfile::tempdir().unwrap();
    {
        let mut log = open_log(d.path());
        assert_eq!(block_on(log.read_vote()).unwrap(), None);
        block_on(log.save_vote(&Vote::new(2, 1))).unwrap();
        let mut v = Vote::new(3, 2);
        v.commit();
        block_on(log.save_vote(&v)).unwrap();
        assert_eq!(block_on(log.read_vote()).unwrap(), Some(v));
    }
    let mut v = Vote::new(3, 2);
    v.commit();
    let mut log = open_log(d.path());
    assert_eq!(block_on(log.read_vote()).unwrap(), Some(v));
    drop(log);
    // A crash in the middle of the next save leaves a partial temp file:
    // the previous vote stays.
    std::fs::write(d.path().join("vote.tmp"), [1, 2, 3]).unwrap();
    let mut log = open_log(d.path());
    assert_eq!(block_on(log.read_vote()).unwrap(), Some(v));
    assert!(!d.path().join("vote.tmp").exists());
}

#[test]
fn committed_round_trip_and_damage() {
    let d = tempfile::tempdir().unwrap();
    {
        let mut log = open_log(d.path());
        append(&mut log, (1..=5).map(|i| filler(1, i)));
        assert_eq!(block_on(log.read_committed()).unwrap(), None);
        block_on(log.save_committed(Some(lid(1, 4)))).unwrap();
        block_on(log.save_committed(Some(lid(1, 3)))).unwrap();
    }
    let mut log = open_log(d.path());
    assert_eq!(block_on(log.read_committed()).unwrap(), Some(lid(1, 3)));
    drop(log);
    let p = d.path().join("committed");
    let mut b = std::fs::read(&p).unwrap();
    b[9] ^= 1;
    std::fs::write(&p, &b).unwrap();
    let mut log = open_log(d.path());
    assert_eq!(block_on(log.read_committed()).unwrap(), None);
    // A commit index past the end of the log is ignored.
    block_on(log.save_committed(Some(lid(1, 5)))).unwrap();
    block_on(log.truncate(lid(1, 5))).unwrap();
    drop(log);
    let mut log = open_log(d.path());
    assert_eq!(block_on(log.read_committed()).unwrap(), None);
}

#[test]
fn truncate_and_purge_persist() {
    let d = tempfile::tempdir().unwrap();
    let mut log = open_log(d.path());
    append(&mut log, (1..=40).map(|i| filler(1, i)));
    let before = segments(d.path()).len();
    block_on(log.truncate(lid(1, 25))).unwrap();
    assert_eq!(state(&mut log), (None, Some(24)));
    assert!(segments(d.path()).len() < before);
    // Conflicting entries get replaced by a new term.
    append(&mut log, (25..=27).map(|i| filler(2, i)));
    block_on(log.purge(lid(1, 10))).unwrap();
    assert_eq!(state(&mut log), (Some(10), Some(27)));
    let all = read_all(&mut log);
    assert_eq!(indexes(&all), (11..=27).collect::<Vec<_>>());
    assert_eq!(all.last().unwrap().log_id, lid(2, 27));
    assert!(segments(d.path())[0].to_str().unwrap().contains(".seg"));
    drop(log);

    let mut log = open_log(d.path());
    assert_eq!(state(&mut log), (Some(10), Some(27)));
    assert_eq!(read_all(&mut log), all);
    // Nothing below the purge point is readable.
    assert!(block_on(log.try_get_log_entries(5..12)).unwrap().len() == 1);

    // Truncate everything above the purge point, then purge past the end
    // (snapshot install on a lagging follower).
    block_on(log.truncate(lid(1, 11))).unwrap();
    assert_eq!(state(&mut log), (Some(10), Some(10)));
    block_on(log.purge(lid(3, 100))).unwrap();
    assert_eq!(state(&mut log), (Some(100), Some(100)));
    assert!(segments(d.path()).is_empty());
    assert!(block_on(log.blocking_append([filler(3, 102)])).is_err());
    append(&mut log, [filler(3, 101), filler(3, 102)]);
    drop(log);
    let mut log = open_log(d.path());
    assert_eq!(state(&mut log), (Some(100), Some(102)));
    assert_eq!(indexes(&read_all(&mut log)), vec![101, 102]);
}

#[test]
fn purge_everything_then_reopen() {
    let d = tempfile::tempdir().unwrap();
    let mut log = open_log(d.path());
    append(&mut log, (0..=9).map(|i| filler(1, i)));
    block_on(log.purge(lid(1, 9))).unwrap();
    assert_eq!(state(&mut log), (Some(9), Some(9)));
    assert!(read_all(&mut log).is_empty());
    drop(log);
    let mut log = open_log(d.path());
    assert_eq!(state(&mut log), (Some(9), Some(9)));
    append(&mut log, [filler(1, 10)]);
    assert_eq!(indexes(&read_all(&mut log)), vec![10]);
}

/// A crash after the purge marker was written but before the segments were
/// deleted: reopening deletes them and ignores their entries.
#[test]
fn crash_mid_purge() {
    let d = tempfile::tempdir().unwrap();
    let saved = tempfile::tempdir().unwrap();
    {
        let mut log = open_log(d.path());
        append(&mut log, (1..=30).map(|i| filler(1, i)));
    }
    copy_dir(d.path(), saved.path());
    {
        let mut log = open_log(d.path());
        block_on(log.purge(lid(1, 20))).unwrap();
    }
    // Put the deleted segments back, as if the deletes never happened.
    for p in segments(saved.path()) {
        let to = d.path().join(p.file_name().unwrap());
        if !to.exists() {
            std::fs::copy(&p, &to).unwrap();
        }
    }
    let mut log = open_log(d.path());
    assert_eq!(state(&mut log), (Some(20), Some(30)));
    assert_eq!(indexes(&read_all(&mut log)), (21..=30).collect::<Vec<_>>());
    let n = segments(d.path()).len();
    assert!(n < segments(saved.path()).len());
}

/// A crash in the middle of a truncate that spans segments: the later
/// segments were removed newest first, so the log is still a consecutive
/// prefix (openraft repeats the truncate).
#[test]
fn crash_mid_truncate_leaves_a_consecutive_log() {
    let d = tempfile::tempdir().unwrap();
    {
        let mut log = open_log(d.path());
        append(&mut log, (1..=30).map(|i| filler(1, i)));
    }
    let segs = segments(d.path());
    // Simulate: only the newest segment got removed.
    std::fs::remove_file(segs.last().unwrap()).unwrap();
    let mut log = open_log(d.path());
    let (_, last) = state(&mut log);
    let all = read_all(&mut log);
    assert_eq!(indexes(&all), (1..=last.unwrap()).collect::<Vec<_>>());
    block_on(log.truncate(lid(1, 5))).unwrap();
    assert_eq!(state(&mut log), (None, Some(4)));
}

#[test]
fn has_state_detects_existing_data() {
    let d = tempfile::tempdir().unwrap();
    assert!(!crate::storage::has_state(d.path()).unwrap());
    let log_dir = crate::storage::log_dir(d.path());
    {
        let _log = LogStore::open(&log_dir, small_log_opts()).unwrap();
    }
    assert!(!crate::storage::has_state(d.path()).unwrap());
    let mut log = LogStore::open(&log_dir, small_log_opts()).unwrap();
    block_on(log.save_vote(&Vote::new(1, 1))).unwrap();
    assert!(crate::storage::has_state(d.path()).unwrap());
}

#[test]
fn log_ids_keep_leader_and_term() {
    let d = tempfile::tempdir().unwrap();
    let mut log = open_log(d.path());
    let e = Entry::<TypeConfig> {
        log_id: LogId::new(CommittedLeaderId::new(7, 3), 1),
        payload: EntryPayload::Blank,
    };
    append(&mut log, [e.clone()]);
    drop(log);
    let mut log = open_log(d.path());
    assert_eq!(read_all(&mut log), vec![e]);
}

// ------------------------------------------------------ group commit (P3-FD)

fn big_segments(dir: &Path) -> LogStore {
    LogStore::open(
        dir,
        LogOptions {
            segment_size: 64 << 20,
            max_read_bytes: 1 << 20,
        },
    )
    .unwrap()
}

/// Spawns one `blocking_append` task per index of `range` (in order, so
/// the indexes reach the store consecutively) and waits until all of them
/// are queued at the (paused) flusher.
async fn queue_appends(
    log: &LogStore,
    range: std::ops::RangeInclusive<u64>,
) -> Vec<tokio::task::JoinHandle<Result<(), openraft::StorageError<NodeId>>>> {
    let before = log.flusher().queued();
    let n = range.clone().count();
    let hs: Vec<_> = range
        .map(|i| {
            let mut l = log.clone();
            tokio::spawn(async move { l.blocking_append([filler(1, i)]).await })
        })
        .collect();
    while log.flusher().queued() < before + n {
        assert!(
            !hs.iter().any(|h| h.is_finished()),
            "an append finished without waiting for its sync"
        );
        tokio::task::yield_now().await;
    }
    hs
}

/// Appends queued while a sync is pending are covered by one `fdatasync`,
/// and are readable before it.
#[test]
fn appends_coalesce_into_few_syncs() {
    let d = tempfile::tempdir().unwrap();
    let mut log = big_segments(d.path());
    // Without contention: one sync per append.
    let s0 = log.flusher().syncs();
    for i in 1..=20 {
        append(&mut log, [filler(1, i)]);
    }
    assert_eq!(log.flusher().syncs() - s0, 20);

    block_on(async {
        log.flusher().set_paused(true);
        let hs = queue_appends(&log, 21..=520).await;
        // Written, not yet durable: already readable, not acknowledged.
        let mut r = log.clone();
        let all = r.try_get_log_entries(..).await.unwrap();
        assert_eq!(indexes(&all), (1..=520).collect::<Vec<_>>());
        assert!(hs.iter().all(|h| !h.is_finished()));
        let s1 = log.flusher().syncs();
        log.flusher().set_paused(false);
        for h in hs {
            h.await.unwrap().unwrap();
        }
        assert_eq!(log.flusher().syncs() - s1, 1, "500 appends, one sync");
    });
    drop(log);
    let mut log = big_segments(d.path());
    assert_eq!(state(&mut log), (None, Some(520)));
}

/// A crash after appends were written but before their sync: whatever
/// the file system kept of the unsynced tail (any prefix, or zeros in
/// place of the lost pages), the reopened log holds every acknowledged
/// entry and is consecutive.
#[test]
fn crash_between_write_and_sync() {
    let base = tempfile::tempdir().unwrap();
    let acked_end;
    {
        let mut log = big_segments(base.path());
        append(&mut log, (1..=10).map(|i| filler(1, i)));
        acked_end = std::fs::metadata(segments(base.path())[0].clone())
            .unwrap()
            .len();
        block_on(async {
            log.flusher().set_paused(true);
            let hs = queue_appends(&log, 11..=16).await;
            // The crash: the queued syncs never happen, nothing more is
            // acknowledged.
            log.flusher().crash();
            for h in hs {
                assert!(h.await.unwrap().is_err(), "never acknowledged");
            }
        });
    }
    let segs = segments(base.path());
    assert_eq!(segs.len(), 1);
    let name = segs[0].file_name().unwrap().to_owned();
    let full = std::fs::read(&segs[0]).unwrap();
    assert!(
        full.len() as u64 > acked_end,
        "unsynced records were written"
    );
    let check = |d: &Path, what: &str| {
        let mut log = big_segments(d);
        let (_, last) = state(&mut log);
        let last = last.unwrap();
        assert!((10..=16).contains(&last), "{what}: last {last}");
        let all = read_all(&mut log);
        assert_eq!(indexes(&all), (1..=last).collect::<Vec<_>>(), "{what}");
        assert_eq!(all[9], filler(1, 10), "{what}");
        // The log keeps working after the recovery.
        append(&mut log, [filler(2, last + 1)]);
    };
    for cut in acked_end..=full.len() as u64 {
        // A prefix of the unsynced bytes survived.
        let d = tempfile::tempdir().unwrap();
        copy_dir(base.path(), d.path());
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(d.path().join(&name))
            .unwrap();
        f.set_len(cut).unwrap();
        drop(f);
        check(d.path(), &format!("cut at {cut}"));

        // The size survived but the data from `cut` on did not.
        let d = tempfile::tempdir().unwrap();
        copy_dir(base.path(), d.path());
        let mut bytes = full.clone();
        bytes[cut as usize..].fill(0);
        std::fs::write(d.path().join(&name), &bytes).unwrap();
        check(d.path(), &format!("zeros from {cut}"));
    }
}

/// `truncate`, `purge` and `save_vote` wait until every pending sync is
/// done (and acknowledged) before they touch the files.
#[test]
fn truncate_purge_and_vote_wait_for_pending_syncs() {
    let d = tempfile::tempdir().unwrap();
    let mut log = big_segments(d.path());
    append(&mut log, (1..=5).map(|i| filler(1, i)));
    let order = Arc::new(Mutex::new(Vec::new()));
    block_on(async {
        for (what, first) in [("truncate", 6u64), ("purge", 7), ("vote", 9)] {
            log.flusher().set_paused(true);
            let hs = queue_appends(&log, first..=first + 1).await;
            let mut l = log.clone();
            let o = order.clone();
            let op = tokio::spawn(async move {
                match what {
                    "truncate" => l.truncate(lid(1, 7)).await.unwrap(),
                    "purge" => l.purge(lid(1, 3)).await.unwrap(),
                    _ => l.save_vote(&Vote::new(4, 1)).await.unwrap(),
                }
                o.lock().unwrap().push(what);
            });
            let o = order.clone();
            let appends = tokio::spawn(async move {
                for h in hs {
                    h.await.unwrap().unwrap();
                }
                o.lock().unwrap().push("appends");
            });
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            assert!(!op.is_finished(), "{what} must wait for the pending sync");
            log.flusher().set_paused(false);
            op.await.unwrap();
            appends.await.unwrap();
        }
    });
    assert_eq!(
        *order.lock().unwrap(),
        ["appends", "truncate", "appends", "purge", "appends", "vote"]
    );
    // Truncated at 7 (6 survives), then 7..=10 appended, purged up to 3.
    assert_eq!(state(&mut log), (Some(3), Some(10)));
    drop(log);
    let mut log = big_segments(d.path());
    assert_eq!(state(&mut log), (Some(3), Some(10)));
    assert_eq!(indexes(&read_all(&mut log)), (4..=10).collect::<Vec<_>>());
    assert_eq!(block_on(log.read_vote()).unwrap(), Some(Vote::new(4, 1)));
}

/// A crash with appends queued at the flusher that rolled over to new
/// segments: the rolled-over segments were synced when the next one was
/// started, so the log reopens (an unsynced tail can only be in the last
/// segment) with every acknowledged entry.
#[test]
fn crash_with_pending_syncs_across_a_rollover() {
    let d = tempfile::tempdir().unwrap();
    {
        let mut log = open_log(d.path());
        append(&mut log, (1..=3).map(|i| filler(1, i)));
        let before = segments(d.path()).len();
        block_on(async {
            log.flusher().set_paused(true);
            let hs = queue_appends(&log, 4..=40).await;
            assert!(segments(d.path()).len() > before + 2, "must roll over");
            log.flusher().crash();
            for h in hs {
                assert!(h.await.unwrap().is_err());
            }
        });
    }
    let mut log = open_log(d.path());
    let (_, last) = state(&mut log);
    let last = last.unwrap();
    assert!((3..=40).contains(&last));
    assert_eq!(indexes(&read_all(&mut log)), (1..=last).collect::<Vec<_>>());
}
