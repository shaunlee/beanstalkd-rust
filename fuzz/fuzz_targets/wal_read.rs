//! Binlog recovery (`Wal::open`): any set of `binlog.N` files must open or
//! fail with an error, never panic, and a binlog that opened must reopen
//! with the same live jobs (recovery truncates, deletes and compacts
//! files, so the second open sees what the first one left).
//!
//! Input: a sequence of files, each `flags`, an index (one byte, or eight
//! when the byte is 0xff), a little-endian `u16` length and the bytes. Flag
//! bit 0 prepends a valid segment header, bit 1 rewrites every record's
//! CRC so records get past the checksum.
#![no_main]

use bstk_engine::RecoveredJob;
use bstk_store::{SyncPolicy, Wal, WalOptions};
use libfuzzer_sys::fuzz_target;

const HEADER: [u8; 16] = *b"BSTKWAL\0\x01\0\0\0\0\0\0\0";

fn fix_crcs(seg: &mut [u8], start: usize) {
    let mut pos = start;
    while pos + 8 <= seg.len() {
        let len = u32::from_le_bytes([seg[pos], seg[pos + 1], seg[pos + 2], seg[pos + 3]]) as usize;
        if len == 0 || pos + 8 + len > seg.len() {
            return;
        }
        let crc = crc32c::crc32c_append(
            crc32c::crc32c(&seg[pos..pos + 4]),
            &seg[pos + 8..pos + 8 + len],
        );
        seg[pos + 4..pos + 8].copy_from_slice(&crc.to_le_bytes());
        pos += 8 + len;
    }
}

fn opts(dir: &std::path::Path) -> WalOptions {
    WalOptions {
        dir: dir.to_path_buf(),
        file_size: 4096,
        sync: SyncPolicy::Never,
    }
}

fn by_id(mut jobs: Vec<RecoveredJob>) -> Vec<RecoveredJob> {
    jobs.sort_by_key(|j| j.record.id);
    jobs
}

fuzz_target!(|data: &[u8]| {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut rest = data;
    let mut files = 0;
    let mut top = 0;
    while let [flags, sel, tail @ ..] = rest {
        let (index, tail) = match (*sel, tail) {
            (0xff, [a, b, c, d, e, f, g, h, tail @ ..]) => {
                (u64::from_le_bytes([*a, *b, *c, *d, *e, *f, *g, *h]), tail)
            }
            (s, tail) => (u64::from(s % 8) + 1, tail),
        };
        let [l0, l1, tail @ ..] = tail else { break };
        let len = usize::from(u16::from_le_bytes([*l0, *l1])).min(tail.len());
        let (body, tail) = tail.split_at(len);
        rest = tail;
        if index == 0 {
            continue;
        }
        top = top.max(index);
        let mut seg = Vec::with_capacity(16 + body.len());
        let start = if flags & 1 != 0 {
            seg.extend_from_slice(&HEADER);
            16
        } else {
            0
        };
        seg.extend_from_slice(body);
        if flags & 2 != 0 {
            fix_crcs(&mut seg, start.max(16));
        }
        std::fs::write(dir.path().join(format!("binlog.{index}")), &seg).expect("write segment");
        files += 1;
        if files == 8 {
            break;
        }
    }

    // Segment numbers above 2^62 are refused, so a binlog opened just below
    // that bound may legitimately refuse to reopen once it has numbered new
    // segments past it; only far below it must a reopen succeed.
    let near_bound = top >= (1 << 62) - (1 << 20);
    let first = match Wal::open(opts(dir.path())) {
        Ok((wal, rec)) => {
            drop(wal);
            rec
        }
        Err(_) => return,
    };
    let second = match Wal::open(opts(dir.path())) {
        Ok((_wal, rec)) => rec,
        Err(_) if near_bound => return,
        Err(e) => panic!("a binlog that opened must reopen: {e:?}"),
    };
    let max_live = first.jobs.iter().map(|j| j.record.id).max().unwrap_or(0);
    assert!(
        second.next_id > max_live,
        "next id {} reuses a live id",
        second.next_id
    );
    assert_eq!(
        by_id(first.jobs),
        by_id(second.jobs),
        "reopening changed the live jobs"
    );
});
