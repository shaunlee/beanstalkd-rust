//! Segmented, CRC-checked Raft log (openraft `RaftLogStorage`).
//!
//! # Segment file `<first>.seg` (first = 20-digit first log index)
//!
//! ```text
//! offset 0   magic        8 bytes  b"BSTKRLOG"
//! offset 8   version      u32 LE   1
//! offset 12  flags        u32 LE   0 (reserved)
//! offset 16  first index  u64 LE   must match the file name
//! offset 24  records ...  len u32 | crc u32 | postcard(Entry<TypeConfig>)
//! ```
//!
//! Records hold consecutive log indexes starting at `first`; segments are
//! consecutive too. A segment is started when the current one would grow
//! past `LogOptions::segment_size` (a single larger record gets a segment
//! of its own). New segment files are created with their header synced and
//! the directory synced before any record goes in.
//!
//! # Open
//!
//! In the last segment, the first torn record (short header, zero or
//! overlong length, checksum mismatch, undecodable entry) and everything
//! after it are cut off with a warning: that is the tail of an append that
//! never completed its `fdatasync`, so it was never acknowledged. Any
//! other damage (an earlier segment, a checksum-valid record with the
//! wrong index, a gap between segments, a bad vote or purge marker)
//! refuses to open.
//!
//! # Operations
//!
//! - `append`: one positioned write per segment touched, then one
//!   `fdatasync` (outside the lock), then the callback. Indexes must continue the log;
//!   entries at or below the purge marker are skipped (a snapshot already
//!   covers them).
//! - `truncate(i)`: remove later segments (newest first), then cut the
//!   segment holding `i` at its record; sync.
//! - `purge(id)`: persist the purge marker first, then delete the segments
//!   that only hold entries at or below it (oldest first). Entries at or
//!   below the marker left in a surviving segment are ignored.
//!
//! Reads go through `pread` on the segment files; only record offsets are
//! kept in memory. All state is behind one mutex; openraft serializes
//! writes, and readers (replication tasks) take the mutex briefly.

use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::io;
use std::ops::{Bound, RangeBounds};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use openraft::storage::{LogFlushed, RaftLogStorage};
use openraft::{
    AnyError, Entry, ErrorSubject, ErrorVerb, LogId, LogState, OptionalSend, RaftLogReader,
    StorageError, StorageIOError, Vote,
};

use super::OpenError;
use super::fsutil::{self, REC_HEADER_LEN};
use crate::{NodeId, TypeConfig};

const SEG_MAGIC: [u8; 8] = *b"BSTKRLOG";
const SEG_VERSION: u32 = 1;
const SEG_HEADER_LEN: u64 = 24;

const VOTE_FILE: &str = "vote";
const PURGED_FILE: &str = "purged";
const COMMITTED_FILE: &str = "committed";

/// Log store tuning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogOptions {
    /// A new segment is started when the current one would grow past this
    /// many bytes.
    pub segment_size: u64,
    /// `limited_get_log_entries` stops after this many bytes of records
    /// (always returning at least one entry).
    pub max_read_bytes: u64,
}

impl Default for LogOptions {
    fn default() -> Self {
        LogOptions {
            segment_size: 64 << 20,
            max_read_bytes: 16 << 20,
        }
    }
}

/// Point-in-time log figures for monitoring.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LogMetrics {
    pub segments: u64,
    /// Total size of the segment files.
    pub bytes: u64,
    /// Index of the first entry present, if any.
    pub first_index: Option<u64>,
    /// Index of the last entry present, if any.
    pub last_index: Option<u64>,
    pub last_purged_index: Option<u64>,
}

type Ent = Entry<TypeConfig>;
type Sid = LogId<NodeId>;
type SResult<T> = Result<T, StorageError<NodeId>>;

#[derive(Debug)]
struct Segment {
    first: u64,
    path: PathBuf,
    /// Shared so that `append` can sync it after releasing the lock.
    file: Arc<File>,
    /// Offset of each record; record `k` holds index `first + k`.
    offsets: Vec<u64>,
    /// End of the last record (= file length).
    end: u64,
}

impl Segment {
    fn next(&self) -> u64 {
        self.first + self.offsets.len() as u64
    }

    /// Byte range of record `k`.
    fn range(&self, k: usize) -> (u64, u64) {
        let start = self.offsets[k];
        let end = self.offsets.get(k + 1).copied().unwrap_or(self.end);
        (start, end)
    }
}

#[derive(Debug)]
struct Inner {
    dir: PathBuf,
    _lock: File,
    opts: LogOptions,
    segs: Vec<Segment>,
    purged: Option<Sid>,
    /// Id of the last entry present above `purged`.
    last_entry: Option<Sid>,
    vote: Option<Vote<NodeId>>,
    committed: Option<Sid>,
    committed_file: File,
}

/// The Raft log, vote and commit marker of one node. Cloning gives another
/// handle to the same store (openraft uses clones as log readers).
#[derive(Debug, Clone)]
pub struct LogStore {
    inner: Arc<Mutex<Inner>>,
}

fn seg_name(first: u64) -> String {
    format!("{first:020}.seg")
}

fn parse_seg_name(name: &str) -> Option<u64> {
    let digits = name.strip_suffix(".seg")?;
    if digits.len() != 20 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

fn seg_header(first: u64) -> [u8; SEG_HEADER_LEN as usize] {
    let mut h = [0u8; SEG_HEADER_LEN as usize];
    h[..8].copy_from_slice(&SEG_MAGIC);
    h[8..12].copy_from_slice(&SEG_VERSION.to_le_bytes());
    h[16..24].copy_from_slice(&first.to_le_bytes());
    h
}

fn corrupt(msg: impl Into<String>) -> OpenError {
    OpenError::Corrupt(msg.into())
}

fn sto_err(subject: ErrorSubject<NodeId>, verb: ErrorVerb, e: &io::Error) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::new(subject, verb, AnyError::new(e)),
    }
}

fn logs_err(verb: ErrorVerb, e: &io::Error) -> StorageError<NodeId> {
    sto_err(ErrorSubject::Logs, verb, e)
}

fn invalid(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

fn encode<T: serde::Serialize>(v: &T) -> io::Result<Vec<u8>> {
    postcard::to_allocvec(v).map_err(|e| invalid(format!("encode: {e}")))
}

fn decode<T: serde::de::DeserializeOwned>(b: &[u8]) -> io::Result<T> {
    postcard::from_bytes(b).map_err(|e| invalid(format!("decode: {e}")))
}

/// Read a single-record file holding a postcard value; `None` if absent.
fn read_value_file<T: serde::de::DeserializeOwned>(path: &Path) -> Result<Option<T>, OpenError> {
    match fsutil::read_record_file(path) {
        Ok(None) => Ok(None),
        Ok(Some(p)) => decode(&p)
            .map(Some)
            .map_err(|e| corrupt(format!("{}: {e}", path.display()))),
        Err(e) if e.kind() == io::ErrorKind::InvalidData => Err(corrupt(e.to_string())),
        Err(e) => Err(OpenError::Io(e)),
    }
}

/// Result of scanning one segment file.
struct Scanned {
    seg: Segment,
    last: Option<Sid>,
}

/// Scan a segment. `is_last` allows cutting off a torn tail; otherwise any
/// damage is corruption. Returns `None` if the (last) segment has no valid
/// header and was removed.
fn scan_segment(path: &Path, first: u64, is_last: bool) -> Result<Option<Scanned>, OpenError> {
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    let len = file.metadata()?.len();
    let mut hdr = [0u8; SEG_HEADER_LEN as usize];
    let hdr_ok = len >= SEG_HEADER_LEN && {
        file.read_exact_at(&mut hdr, 0)?;
        hdr == seg_header(first)
    };
    if !hdr_ok {
        if is_last && len <= SEG_HEADER_LEN {
            tracing::warn!(
                "{}: incomplete segment header ({len} bytes); removing the segment",
                path.display()
            );
            drop(file);
            std::fs::remove_file(path)?;
            return Ok(None);
        }
        return Err(corrupt(format!("{}: bad segment header", path.display())));
    }
    let mut offsets = Vec::new();
    let mut off = SEG_HEADER_LEN;
    let mut last = None;
    while off < len {
        let bad = match fsutil::read_record_at(&file, off, len)? {
            Ok((payload, total)) => match decode::<Ent>(&payload) {
                Ok(ent) => {
                    let want = first + offsets.len() as u64;
                    if ent.log_id.index != want {
                        return Err(corrupt(format!(
                            "{}: record at offset {off} holds index {}, expected {want}",
                            path.display(),
                            ent.log_id.index
                        )));
                    }
                    offsets.push(off);
                    last = Some(ent.log_id);
                    off += total;
                    continue;
                }
                Err(e) => e.to_string(),
            },
            Err(why) => why.to_string(),
        };
        if !is_last {
            return Err(corrupt(format!(
                "{}: {bad} at offset {off}",
                path.display()
            )));
        }
        tracing::warn!(
            "{}: {bad} at offset {off}; discarding the rest of the segment \
             ({} bytes, an append that never completed)",
            path.display(),
            len - off
        );
        file.set_len(off)?;
        fsutil::sync_data(&file)?;
        break;
    }
    Ok(Some(Scanned {
        seg: Segment {
            first,
            path: path.to_path_buf(),
            file: Arc::new(file),
            offsets,
            end: off,
        },
        last,
    }))
}

impl LogStore {
    /// Lock `dir` (creating it if needed) and load the log. See the module
    /// docs for the corruption rules.
    pub fn open(dir: &Path, opts: LogOptions) -> Result<LogStore, OpenError> {
        let lock = fsutil::lock_dir(dir)?;
        fsutil::remove_tmp_files(dir)?;
        let vote: Option<Vote<NodeId>> = read_value_file(&dir.join(VOTE_FILE))?;
        let purged: Option<Sid> = read_value_file(&dir.join(PURGED_FILE))?;

        let mut firsts = Vec::new();
        for ent in std::fs::read_dir(dir)? {
            let ent = ent?;
            if let Some(first) = ent.file_name().to_str().and_then(parse_seg_name) {
                firsts.push(first);
            }
        }
        firsts.sort_unstable();

        let mut segs: Vec<Segment> = Vec::new();
        let mut last_entry = None;
        let mut dir_changed = false;
        for (i, &first) in firsts.iter().enumerate() {
            let path = dir.join(seg_name(first));
            let Some(sc) = scan_segment(&path, first, i + 1 == firsts.len())? else {
                dir_changed = true;
                continue;
            };
            if sc.seg.offsets.is_empty() {
                // Nothing in it (a crash right after creating it, or all
                // of it was a torn tail).
                drop(sc);
                std::fs::remove_file(&path)?;
                dir_changed = true;
                continue;
            }
            if let Some(prev) = segs.last()
                && prev.next() != first
            {
                return Err(corrupt(format!(
                    "log gap: {} ends before index {}, next segment starts at {first}",
                    prev.path.display(),
                    prev.next()
                )));
            }
            last_entry = sc.last;
            segs.push(sc.seg);
        }

        // Drop segments entirely at or below the purge marker (a crash
        // between writing the marker and deleting them).
        if let Some(p) = purged {
            while segs.first().is_some_and(|s| s.next() <= p.index + 1) {
                let s = segs.remove(0);
                std::fs::remove_file(&s.path)?;
                dir_changed = true;
            }
            if let Some(s) = segs.first()
                && s.first > p.index + 1
            {
                return Err(corrupt(format!(
                    "log gap: purged up to {}, first segment starts at {}",
                    p.index, s.first
                )));
            }
            if segs.is_empty() {
                last_entry = None;
            }
        }
        if dir_changed {
            fsutil::sync_dir(dir)?;
        }

        let committed_path = dir.join(COMMITTED_FILE);
        let committed = match fsutil::read_record_file(&committed_path) {
            Ok(Some(p)) => match decode::<Option<Sid>>(&p) {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!("{}: {e}; ignoring it", committed_path.display());
                    None
                }
            },
            Ok(None) => None,
            Err(e) if e.kind() == io::ErrorKind::InvalidData => {
                tracing::warn!("{e}; ignoring the saved commit index");
                None
            }
            Err(e) => return Err(OpenError::Io(e)),
        };
        let top = last_entry.or(purged).map(|l| l.index);
        let committed = match committed {
            Some(c) if top.is_none_or(|t| c.index > t) => {
                tracing::warn!(
                    "saved commit index {} is past the end of the log; ignoring it",
                    c.index
                );
                None
            }
            c => c,
        };
        let committed_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(&committed_path)?;

        Ok(LogStore {
            inner: Arc::new(Mutex::new(Inner {
                dir: dir.to_path_buf(),
                _lock: lock,
                opts,
                segs,
                purged,
                last_entry,
                vote,
                committed,
                committed_file,
            })),
        })
    }

    fn lock(&self) -> SResult<MutexGuard<'_, Inner>> {
        self.inner.lock().map_err(|_| {
            logs_err(
                ErrorVerb::Read,
                &io::Error::other("log store mutex poisoned"),
            )
        })
    }

    /// Current log figures (for `/metrics`).
    pub fn metrics(&self) -> LogMetrics {
        match self.inner.lock() {
            Ok(g) => g.metrics(),
            Err(_) => LogMetrics::default(),
        }
    }
}

impl Inner {
    fn metrics(&self) -> LogMetrics {
        LogMetrics {
            segments: self.segs.len() as u64,
            bytes: self.segs.iter().map(|s| s.end).sum(),
            first_index: self.first_live(),
            last_index: self.last_entry.map(|l| l.index),
            last_purged_index: self.purged.map(|p| p.index),
        }
    }

    /// Index of the first entry present, if any.
    fn first_live(&self) -> Option<u64> {
        let last = self.last_entry?;
        let first = self.segs.first()?.first;
        let lo = self.purged.map_or(first, |p| first.max(p.index + 1));
        (lo <= last.index).then_some(lo)
    }

    /// The index the next appended entry must have, if constrained.
    fn next_index(&self) -> Option<u64> {
        match (self.last_entry, self.purged) {
            (Some(l), _) => Some(l.index + 1),
            (None, Some(p)) => Some(p.index + 1),
            (None, None) => None,
        }
    }

    /// Segment position and record number of `index` (must be present).
    fn locate(&self, index: u64) -> Option<(usize, usize)> {
        let p = self.segs.partition_point(|s| s.first <= index);
        let si = p.checked_sub(1)?;
        let k = usize::try_from(index - self.segs[si].first).ok()?;
        (k < self.segs[si].offsets.len()).then_some((si, k))
    }

    /// Read entries `[lo, hi)` (all present), stopping once `max_bytes`
    /// have been read (at least one entry).
    fn read_entries(&self, lo: u64, hi: u64, max_bytes: u64) -> io::Result<Vec<Ent>> {
        let mut out = Vec::new();
        let mut idx = lo;
        let mut bytes = 0u64;
        while idx < hi {
            let (si, k) = self
                .locate(idx)
                .ok_or_else(|| invalid(format!("log index {idx} not found")))?;
            let seg = &self.segs[si];
            let n_in_seg = seg.offsets.len() - k;
            let want = usize::try_from(hi - idx)
                .unwrap_or(usize::MAX)
                .min(n_in_seg);
            // Records k .. k+cnt, bounded by max_bytes (at least one).
            let mut cnt = 0;
            let start = seg.offsets[k];
            let mut end = start;
            while cnt < want {
                let (_, e) = seg.range(k + cnt);
                if cnt > 0 && bytes + (e - start) > max_bytes {
                    break;
                }
                end = e;
                cnt += 1;
            }
            let mut buf = vec![0u8; (end - start) as usize];
            seg.file.read_exact_at(&mut buf, start)?;
            let mut pos = 0;
            for j in 0..cnt {
                let (payload, total) = fsutil::parse_record(&buf[pos..]).map_err(|why| {
                    invalid(format!(
                        "{}: {why} at offset {}",
                        seg.path.display(),
                        seg.offsets[k + j]
                    ))
                })?;
                let ent: Ent = decode(payload)?;
                if ent.log_id.index != idx {
                    return Err(invalid(format!(
                        "{}: expected index {idx}, found {}",
                        seg.path.display(),
                        ent.log_id.index
                    )));
                }
                out.push(ent);
                pos += total;
                idx += 1;
            }
            bytes += end - start;
            if bytes >= max_bytes {
                break;
            }
        }
        Ok(out)
    }

    fn entry_id(&self, index: u64) -> io::Result<Sid> {
        let e = self.read_entries(index, index + 1, u64::MAX)?;
        e.first()
            .map(|e| e.log_id)
            .ok_or_else(|| invalid(format!("log index {index} not found")))
    }

    fn new_segment(&mut self, first: u64) -> io::Result<()> {
        let path = self.dir.join(seg_name(first));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)?;
        file.write_all_at(&seg_header(first), 0)?;
        fsutil::sync_data(&file)?;
        fsutil::sync_dir(&self.dir)?;
        self.segs.push(Segment {
            first,
            path,
            file: Arc::new(file),
            offsets: Vec::new(),
            end: SEG_HEADER_LEN,
        });
        Ok(())
    }

    /// Write `buf` (records at `offs`, relative to the buffer) to the end
    /// of the last segment.
    fn flush(&mut self, buf: &mut Vec<u8>, offs: &mut Vec<u64>) -> io::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        let seg = self
            .segs
            .last_mut()
            .ok_or_else(|| io::Error::other("no current segment"))?;
        seg.file.write_all_at(buf, seg.end)?;
        let base = seg.end;
        seg.offsets.extend(offs.iter().map(|o| base + o));
        seg.end += buf.len() as u64;
        buf.clear();
        offs.clear();
        Ok(())
    }

    /// Write `entries`; returns the segment file that still needs an
    /// `fdatasync` (done by the caller after releasing the lock). Segments
    /// that are rolled over are synced here.
    fn append(&mut self, entries: Vec<Ent>) -> io::Result<Option<Arc<File>>> {
        let mut buf = Vec::new();
        let mut offs = Vec::new();
        let mut unsynced: Option<usize> = None;
        for ent in entries {
            let idx = ent.log_id.index;
            if self.purged.is_some_and(|p| idx <= p.index) {
                // Already covered by a snapshot (openraft purges up to the
                // state machine's last applied id when the log is behind).
                tracing::debug!("not appending log index {idx}: at or below the purge point");
                continue;
            }
            if let Some(want) = self.next_index()
                && idx != want
            {
                return Err(invalid(format!(
                    "non-consecutive append: got index {idx}, expected {want}"
                )));
            }
            let payload = encode(&ent)?;
            let rec_len = REC_HEADER_LEN + payload.len() as u64;
            let pending = buf.len() as u64;
            let need_new = match self.segs.last() {
                None => true,
                Some(s) => {
                    // A segment left over below the purge marker cannot be
                    // continued at a different index. The records of this
                    // batch buffered for the segment (`offs`) are not in
                    // `s.offsets` until `flush`, so they count here.
                    s.next() + offs.len() as u64 != idx
                        || ((!s.offsets.is_empty() || pending > 0)
                            && s.end + pending + rec_len > self.opts.segment_size)
                }
            };
            if need_new {
                self.flush(&mut buf, &mut offs)?;
                if let Some(si) = unsynced.take() {
                    fsutil::sync_data(&self.segs[si].file)?;
                }
                if let Some(s) = self.segs.last()
                    && s.next() != idx
                {
                    // Only possible when everything in the segments is at
                    // or below the purge marker.
                    self.drop_purged_segments()?;
                    if !self.segs.is_empty() {
                        return Err(invalid(format!(
                            "cannot append index {idx} after segment {}",
                            self.segs[self.segs.len() - 1].path.display()
                        )));
                    }
                }
                self.new_segment(idx)?;
            }
            offs.push(buf.len() as u64);
            fsutil::encode_record(&payload, &mut buf)?;
            unsynced = Some(self.segs.len() - 1);
            self.last_entry = Some(ent.log_id);
        }
        self.flush(&mut buf, &mut offs)?;
        Ok(unsynced.map(|si| self.segs[si].file.clone()))
    }

    /// Delete leading segments holding only entries at or below the purge
    /// marker, oldest first.
    fn drop_purged_segments(&mut self) -> io::Result<()> {
        let Some(p) = self.purged else {
            return Ok(());
        };
        let mut changed = false;
        while self.segs.first().is_some_and(|s| s.next() <= p.index + 1) {
            let s = self.segs.remove(0);
            std::fs::remove_file(&s.path)?;
            changed = true;
        }
        if changed {
            fsutil::sync_dir(&self.dir)?;
        }
        Ok(())
    }

    fn truncate(&mut self, since: u64) -> io::Result<()> {
        let Some(last) = self.last_entry else {
            return Ok(());
        };
        if since > last.index {
            return Ok(());
        }
        if let Some(p) = self.purged
            && since <= p.index
        {
            return Err(invalid(format!(
                "truncate at {since} is at or below the purged index {}",
                p.index
            )));
        }
        let mut changed = false;
        while self.segs.last().is_some_and(|s| s.first >= since) {
            if let Some(s) = self.segs.pop() {
                std::fs::remove_file(&s.path)?;
                changed = true;
            }
        }
        if changed {
            fsutil::sync_dir(&self.dir)?;
        }
        if let Some(seg) = self.segs.last_mut() {
            let k = (since - seg.first) as usize;
            if k < seg.offsets.len() {
                let cut = seg.offsets[k];
                seg.file.set_len(cut)?;
                fsutil::sync_data(&seg.file)?;
                seg.offsets.truncate(k);
                seg.end = cut;
            }
        }
        let lo = self.first_live_below(since);
        self.last_entry = match lo {
            Some(_) => Some(self.entry_id(since - 1)?),
            None => None,
        };
        Ok(())
    }

    /// Whether an entry below `since` is still present.
    fn first_live_below(&self, since: u64) -> Option<u64> {
        let first = self.segs.first()?.first;
        let lo = self.purged.map_or(first, |p| first.max(p.index + 1));
        let end = self.segs.last()?.next();
        (lo < since && lo < end).then_some(lo)
    }

    fn purge(&mut self, upto: Sid) -> io::Result<()> {
        if self.purged.is_some_and(|p| p.index >= upto.index) {
            return Ok(());
        }
        fsutil::atomic_write(&self.dir, PURGED_FILE, &fsutil::record(&encode(&upto)?)?)?;
        self.purged = Some(upto);
        if self.last_entry.is_some_and(|l| l.index <= upto.index) {
            self.last_entry = None;
        }
        self.drop_purged_segments()
    }

    fn save_vote(&mut self, vote: &Vote<NodeId>) -> io::Result<()> {
        fsutil::atomic_write(&self.dir, VOTE_FILE, &fsutil::record(&encode(vote)?)?)?;
        self.vote = Some(*vote);
        Ok(())
    }

    fn save_committed(&mut self, committed: Option<Sid>) -> io::Result<()> {
        // A hint only (see the module docs of `storage`): overwritten in
        // place and never synced; a torn write fails its checksum and
        // reads back as `None`.
        let rec = fsutil::record(&encode(&committed)?)?;
        self.committed_file.write_all_at(&rec, 0)?;
        self.committed = committed;
        Ok(())
    }

    fn try_get(&self, range: (Bound<u64>, Bound<u64>)) -> io::Result<Vec<Ent>> {
        let (Some(first), Some(last)) = (self.first_live(), self.last_entry) else {
            return Ok(Vec::new());
        };
        let lo = match range.0 {
            Bound::Included(x) => x,
            Bound::Excluded(x) => x.saturating_add(1),
            Bound::Unbounded => 0,
        }
        .max(first);
        let hi = match range.1 {
            Bound::Included(x) => x.saturating_add(1),
            Bound::Excluded(x) => x,
            Bound::Unbounded => u64::MAX,
        }
        .min(last.index + 1);
        if lo >= hi {
            return Ok(Vec::new());
        }
        self.read_entries(lo, hi, u64::MAX)
    }
}

impl crate::status::StatusSource for LogStore {
    /// Answered from memory (the vote, log bounds and commit hint mirror
    /// what is on disk).
    fn status(&self) -> crate::status::NodeStatus {
        // A poisoned lock still holds the last state (never report "no
        // state" for a node that has some).
        let g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        crate::status::NodeStatus {
            vote: g.vote,
            last_log_id: g.last_entry.or(g.purged),
            committed: g.committed,
            has_state: g.vote.is_some() || g.last_entry.is_some() || g.purged.is_some(),
        }
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> SResult<Vec<Ent>> {
        let g = self.lock()?;
        g.try_get((range.start_bound().cloned(), range.end_bound().cloned()))
            .map_err(|e| logs_err(ErrorVerb::Read, &e))
    }

    async fn limited_get_log_entries(&mut self, start: u64, end: u64) -> SResult<Vec<Ent>> {
        let g = self.lock()?;
        let (Some(first), Some(last)) = (g.first_live(), g.last_entry) else {
            return Ok(Vec::new());
        };
        let lo = start.max(first);
        let hi = end.min(last.index + 1);
        if lo >= hi {
            return Ok(Vec::new());
        }
        g.read_entries(lo, hi, g.opts.max_read_bytes)
            .map_err(|e| logs_err(ErrorVerb::Read, &e))
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = LogStore;

    async fn get_log_state(&mut self) -> SResult<LogState<TypeConfig>> {
        let g = self.lock()?;
        Ok(LogState {
            last_purged_log_id: g.purged,
            last_log_id: g.last_entry.or(g.purged),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> SResult<()> {
        let mut g = self.lock()?;
        g.save_vote(vote)
            .map_err(|e| sto_err(ErrorSubject::Vote, ErrorVerb::Write, &e))
    }

    async fn read_vote(&mut self) -> SResult<Option<Vote<NodeId>>> {
        Ok(self.lock()?.vote)
    }

    async fn save_committed(&mut self, committed: Option<Sid>) -> SResult<()> {
        let mut g = self.lock()?;
        g.save_committed(committed)
            .map_err(|e| sto_err(ErrorSubject::Store, ErrorVerb::Write, &e))
    }

    async fn read_committed(&mut self) -> SResult<Option<Sid>> {
        Ok(self.lock()?.committed)
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<TypeConfig>) -> SResult<()>
    where
        I: IntoIterator<Item = Ent> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries: Vec<Ent> = entries.into_iter().collect();
        // Readers (replication) are not blocked by the sync: the lock is
        // released first. openraft serializes writes, so nothing can
        // truncate the file in between.
        let res = match self.lock() {
            Ok(mut g) => g.append(entries),
            Err(e) => {
                callback.log_io_completed(Err(io::Error::other("log store mutex poisoned")));
                return Err(e);
            }
        };
        let res = res.and_then(|f| f.map_or(Ok(()), |f| fsutil::sync_data(&f)));
        match res {
            Ok(()) => {
                callback.log_io_completed(Ok(()));
                Ok(())
            }
            Err(e) => {
                let err = logs_err(ErrorVerb::Write, &e);
                callback.log_io_completed(Err(e));
                Err(err)
            }
        }
    }

    async fn truncate(&mut self, log_id: Sid) -> SResult<()> {
        let mut g = self.lock()?;
        g.truncate(log_id.index)
            .map_err(|e| logs_err(ErrorVerb::Delete, &e))
    }

    async fn purge(&mut self, log_id: Sid) -> SResult<()> {
        let mut g = self.lock()?;
        g.purge(log_id).map_err(|e| logs_err(ErrorVerb::Delete, &e))
    }
}
