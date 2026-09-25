//! The open log: segment list, job index, space accounting, writes,
//! compaction and garbage collection.
//!
//! # Segments
//!
//! `segs` holds every segment file in index order. `segs[cur]` is the
//! current (write) segment; segments after it are preallocated spares with
//! no records; segments before it are closed (truncated to their records).
//! A new current segment is created on every open.
//!
//! # Job index (memory use)
//!
//! For every live job the store keeps its latest `JobRecord` (needed to
//! write a compaction move), the location of its latest Put record and the
//! bytes it uses: about 120 bytes per job plus hash map overhead. Tubes and
//! bodies are not kept in memory: a compaction move re-reads the job's Put
//! record from its segment with a positioned read and re-stamps it with the
//! latest `JobRecord`. Each segment also keeps a queue of (job, offset)
//! entries for the Puts it holds (16 bytes per entry, cleaned lazily) and
//! the count of live jobs whose latest Put it holds ("anchors").
//!
//! # Space accounting and reservation
//!
//! - `reserved` = one Delete record per live job and per reserved-but-not-
//!   yet-written put, plus the Put records of reserved puts.
//! - `avail` = unwritten bytes of the current segment plus the capacity of
//!   the preallocated spares.
//! - `slack` = worst-case bytes lost to rollover fragmentation (records
//!   never straddle segments): one Delete record per future rollover plus
//!   the pending Put records.
//! - `reserve_put` (and a compaction move) succeeds when
//!   `reserved + n + slack + one segment's capacity <= avail`, allocating
//!   new preallocated segments until it holds. The extra segment is the
//!   spare that unreserved Update records (and fragmentation) consume. If
//!   a segment cannot be allocated (disk full, or the test-only size
//!   limit), `reserve_put` returns false.
//! - A put reservation lasts until the end of the next `append` call: that
//!   call's Put entries consume pending reservations in order, and any left
//!   over (the put was never journaled, e.g. the engine rejected it) are
//!   released.
//! - `append` never checks reservations; if it runs out of preallocated
//!   space it allocates a segment on the spot, and only if that fails does
//!   it return an error.
//! - A Put larger than a whole segment cannot be reserved; if one is
//!   appended anyway it is written at the start of a fresh segment, which
//!   grows past its preallocated size.
//!
//! # Compaction and garbage collection
//!
//! `maintain` computes `ratio = (allocated - live) / live` (integer
//! division) where `allocated` is the bytes of all segment files and
//! `live` is the bytes of live jobs' records (latest Put plus the Updates
//! after it) plus `reserved`; like the reference it then performs
//! `ratio - 1` moves when `ratio >= 2`. A move takes the first live job
//! whose latest Put is in the oldest segment that holds any (and that is
//! at least two segments before the current one), re-reads that Put,
//! verifies its CRC, re-stamps it with the job's latest `JobRecord` and
//! writes it to the current segment. Moves stop early if space for them
//! cannot be secured.
//!
//! GC deletes segments from the head of the list while they are before
//! the current segment and anchor no live job. Before the first unlink it
//! writes out buffered moves and (unless `SyncPolicy::Never`) fsyncs the
//! current segment; with `SyncPolicy::Always` it also fsyncs the directory
//! afterwards.
//!
//! Why every on-disk state replays to the same live set:
//! - A move is a full Put carrying the latest state, written after all of
//!   the job's earlier records, so "last record wins" gives the same
//!   state whether or not the old copy still exists (a crash between the
//!   move and the unlink leaves the job in two files).
//! - Only a prefix of the segment list is ever deleted, and only segments
//!   without the latest Put of any live job. A live job's latest Put and
//!   everything after it survive; records before it that survive are
//!   harmless (an Update for an unknown job is ignored; an older Put is
//!   overridden). A deleted job's Delete record comes after all its other
//!   records, so if any of them survives the Delete survives too: a
//!   deleted job is never resurrected.
//! - A torn move at the tail is dropped by replay, and the old copy is
//!   still there because the unlink only happens after the move was
//!   written (and fsynced unless the policy is `Never`).

use std::collections::{HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::time::Instant;

use bstk_engine::{BinlogStats, JobRecord, JournalEntry, RecoveredJob, Recovery};
use bstk_proto::JobId;

use crate::format::{
    self, DELETE_REC_LEN, JOBREC_OFFSET, Parsed, Rec, SEG_HEADER_LEN, put_rec_len,
};
use crate::replay::{self, segment_path};
use crate::{SyncPolicy, WalError, WalOptions};

const BLOCK: u64 = 4096;

use crate::MAX_FILE_SIZE as MAX_SEGMENT_SIZE;

#[derive(Debug)]
struct Seg {
    index: u64,
    path: PathBuf,
    /// Bytes the file occupies (preallocated size, or truncated length).
    size: u64,
    /// End of the last record (or of the header).
    written: u64,
    /// Live jobs whose latest Put is in this segment.
    anchors: u64,
    /// (job, offset) of Puts written here, in file order; entries go stale
    /// when the job is moved or deleted and are skipped lazily.
    puts: VecDeque<(JobId, u64)>,
    /// Write handle; `Some` for the current segment and the spares.
    file: Option<File>,
}

#[derive(Debug)]
struct JobEntry {
    record: JobRecord,
    seg: u64,
    off: u64,
    put_len: u64,
    used: u64,
}

#[derive(Debug)]
pub(crate) struct Inner {
    dir: PathBuf,
    dir_file: File,
    _lock: File,
    policy: SyncPolicy,
    seg_size: u64,
    /// Test-only cap on the total bytes of segment files.
    limit: Option<u64>,
    segs: VecDeque<Seg>,
    cur: usize,
    next_index: u64,
    jobs: HashMap<JobId, JobEntry>,
    live_bytes: u64,
    /// Sum of `size` over `segs`.
    disk_total: u64,
    pending_puts: u64,
    pending_bytes: u64,
    /// Encoded records not yet written, destined for `segs[cur]` at
    /// offset `buf_start`.
    buf: Vec<u8>,
    buf_start: u64,
    unsynced: bool,
    last_sync: Option<Instant>,
    reader: Option<(u64, File)>,
    records_written: u64,
    records_migrated: u64,
    #[cfg(test)]
    pub(crate) sync_count: u64,
    /// Jobs moved by compaction.
    #[cfg(test)]
    pub(crate) moved: std::collections::HashSet<JobId>,
    /// Every write and fsync, in order (test instrumentation only).
    #[cfg(test)]
    pub(crate) file_ops: Vec<FileOp>,
}

/// A file operation recorded for the ordering tests.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FileOp {
    /// `len` bytes written to `binlog.<seg>` at `off`.
    Write { seg: u64, off: u64, len: u64 },
    /// fdatasync of `binlog.<seg>`.
    Sync { seg: u64 },
    /// fsync of the directory.
    DirSync,
}

fn sync_fd(f: &File) -> io::Result<()> {
    // Plain fdatasync/fsync (not F_FULLFSYNC on macOS), like the reference.
    #[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
    {
        nix::unistd::fdatasync(f.as_raw_fd()).map_err(io::Error::from)
    }
    #[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
    {
        nix::unistd::fsync(f.as_raw_fd()).map_err(io::Error::from)
    }
}

fn sync_dir(f: &File) -> io::Result<()> {
    nix::unistd::fsync(f.as_raw_fd()).map_err(io::Error::from)
}

fn lock_dir(dir: &Path) -> Result<File, WalError> {
    let f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(dir.join("lock"))?;
    match f.try_lock() {
        Ok(()) => Ok(f),
        Err(std::fs::TryLockError::WouldBlock) => Err(WalError::Locked),
        Err(std::fs::TryLockError::Error(e)) => Err(WalError::Io(e)),
    }
}

impl Inner {
    pub(crate) fn open(
        opts: WalOptions,
        limit: Option<u64>,
    ) -> Result<(Inner, Recovery), WalError> {
        let seg_size = segment_size(opts.file_size)?;
        std::fs::create_dir_all(&opts.dir)?;
        let lock = lock_dir(&opts.dir)?;
        let dir_file = File::open(&opts.dir)?;
        let scan = replay::scan(&opts.dir)?;
        if let Some((index, off, why)) = scan.torn {
            let len = scan
                .segs
                .iter()
                .find(|s| s.index == index)
                .map_or(0, |s| s.file_len);
            tracing::warn!(
                "binlog.{index}: {why} at offset {off}; discarding the rest of the \
                 segment (up to {} bytes). Unsynced writes may have been lost.",
                len.saturating_sub(off)
            );
        }

        // Replay succeeded: fix up the directory. Segments with records
        // are truncated to their valid end (this drops a torn tail);
        // segments without records are deleted.
        let mut segs = VecDeque::new();
        let mut changed = false;
        for s in &scan.segs {
            if !s.has_records {
                std::fs::remove_file(&s.path)?;
                changed = true;
                continue;
            }
            if s.file_len > s.valid_end {
                let f = OpenOptions::new().write(true).open(&s.path)?;
                f.set_len(s.valid_end)?;
                sync_fd(&f)?;
            }
            segs.push_back(Seg {
                index: s.index,
                path: s.path.clone(),
                size: s.valid_end,
                written: s.valid_end,
                anchors: 0,
                puts: VecDeque::new(),
                file: None,
            });
        }
        if changed {
            sync_dir(&dir_file)?;
        }

        let mut jobs = HashMap::with_capacity(scan.jobs.len());
        let mut recovered = Vec::with_capacity(scan.jobs.len());
        let mut anchored: Vec<(u64, u64, JobId)> = Vec::with_capacity(scan.jobs.len());
        let mut live_bytes = 0;
        for (id, j) in scan.jobs {
            live_bytes += j.used;
            anchored.push((j.seg, j.off, id));
            jobs.insert(
                id,
                JobEntry {
                    record: j.record.clone(),
                    seg: j.seg,
                    off: j.off,
                    put_len: j.put_len,
                    used: j.used,
                },
            );
            recovered.push((
                j.order,
                RecoveredJob {
                    record: j.record,
                    tube: j.tube,
                    body: j.body,
                },
            ));
        }
        recovered.sort_unstable_by_key(|(o, _)| *o);
        anchored.sort_unstable();
        for (seg, off, id) in anchored {
            if let Ok(p) = segs.binary_search_by_key(&seg, |s: &Seg| s.index) {
                segs[p].anchors += 1;
                segs[p].puts.push_back((id, off));
            }
        }

        let mut w = Inner {
            dir: opts.dir,
            dir_file,
            _lock: lock,
            policy: opts.sync,
            seg_size,
            limit,
            segs,
            cur: 0,
            next_index: scan.max_index + 1,
            jobs,
            live_bytes,
            disk_total: 0,
            pending_puts: 0,
            pending_bytes: 0,
            buf: Vec::new(),
            buf_start: SEG_HEADER_LEN,
            unsynced: false,
            last_sync: None,
            reader: None,
            records_written: 0,
            records_migrated: 0,
            #[cfg(test)]
            sync_count: 0,
            #[cfg(test)]
            moved: Default::default(),
            #[cfg(test)]
            file_ops: Vec::new(),
        };
        w.disk_total = w.segs.iter().map(|s| s.size).sum();
        w.allocate()?;
        w.cur = w.segs.len() - 1;
        if !w.ensure_room(0, false) {
            return Err(WalError::Io(io::Error::new(
                io::ErrorKind::StorageFull,
                "cannot preallocate binlog space for the recovered jobs",
            )));
        }
        w.gc()?;
        let recovery = Recovery {
            jobs: recovered.into_iter().map(|(_, j)| j).collect(),
            next_id: scan.next_id,
            tube_order: scan.tube_order,
        };
        Ok((w, recovery))
    }

    fn capacity(&self) -> u64 {
        self.seg_size - SEG_HEADER_LEN
    }

    fn future_count(&self) -> u64 {
        (self.segs.len() - 1 - self.cur) as u64
    }

    fn reserved(&self) -> u64 {
        DELETE_REC_LEN * (self.jobs.len() as u64 + self.pending_puts) + self.pending_bytes
    }

    fn avail(&self) -> u64 {
        let c = &self.segs[self.cur];
        c.size.saturating_sub(c.written) + self.future_count() * self.capacity()
    }

    fn slack(&self) -> u64 {
        DELETE_REC_LEN * (self.future_count() + 1) + self.pending_bytes
    }

    fn disk_bytes(&self) -> u64 {
        self.disk_total
    }

    /// Make sure `n` more bytes can be reserved (while keeping the spare
    /// if `spare`), allocating segments as needed. False if allocation
    /// fails.
    fn ensure_room(&mut self, n: u64, spare: bool) -> bool {
        loop {
            let spare = if spare { self.capacity() } else { 0 };
            let need = self.reserved() + n + self.slack() + spare;
            if need <= self.avail() {
                return true;
            }
            if self.allocate().is_err() {
                return false;
            }
        }
    }

    /// Create and preallocate the next segment file as a spare.
    fn allocate(&mut self) -> Result<(), WalError> {
        if let Some(limit) = self.limit
            && self.disk_bytes() + self.seg_size > limit
        {
            return Err(WalError::Io(io::Error::new(
                io::ErrorKind::StorageFull,
                "binlog size limit reached",
            )));
        }
        let index = self.next_index;
        let path = segment_path(&self.dir, index);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)?;
        self.next_index += 1;
        if let Err(e) = preallocate(&mut file, self.seg_size) {
            drop(file);
            let _ = std::fs::remove_file(&path);
            return Err(e.into());
        }
        if self.policy != SyncPolicy::Never {
            sync_dir(&self.dir_file)?;
            #[cfg(test)]
            self.file_ops.push(FileOp::DirSync);
        }
        self.disk_total += self.seg_size;
        self.segs.push_back(Seg {
            index,
            path,
            size: self.seg_size,
            written: SEG_HEADER_LEN,
            anchors: 0,
            puts: VecDeque::new(),
            file: Some(file),
        });
        Ok(())
    }

    pub(crate) fn reserve_put(&mut self, tube_len: usize, body_len: usize) -> bool {
        let put = put_rec_len(tube_len, body_len);
        if put > self.capacity() {
            return false;
        }
        // Count the new put as pending while checking so the slack covers
        // its fragmentation too.
        self.pending_bytes += put;
        let ok = self.ensure_room(DELETE_REC_LEN, true);
        self.pending_bytes -= put;
        if ok {
            self.pending_puts += 1;
            self.pending_bytes += put;
        }
        ok
    }

    /// Reserve `len` bytes at the end of the current segment, rolling to
    /// the next one if it does not fit. Returns the record's offset.
    fn place(&mut self, len: u64) -> Result<u64, WalError> {
        let c = &self.segs[self.cur];
        if c.written > SEG_HEADER_LEN && c.written + len > c.size {
            self.roll()?;
        }
        let c = &mut self.segs[self.cur];
        let off = c.written;
        c.written += len;
        // A record bigger than a whole segment grows the file.
        if c.written > c.size {
            self.disk_total += c.written - c.size;
            c.size = c.written;
        }
        Ok(off)
    }

    fn roll(&mut self) -> Result<(), WalError> {
        self.flush()?;
        if self.unsynced && self.policy != SyncPolicy::Never {
            self.sync_cur()?;
        }
        if self.cur + 1 >= self.segs.len() {
            self.allocate()?;
        }
        let c = &mut self.segs[self.cur];
        if let Some(f) = c.file.take()
            && c.size > c.written
        {
            f.set_len(c.written)?;
            self.disk_total -= c.size - c.written;
            c.size = c.written;
        }
        self.cur += 1;
        self.buf_start = self.segs[self.cur].written;
        self.unsynced = false;
        Ok(())
    }

    fn flush(&mut self) -> Result<(), WalError> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let c = &self.segs[self.cur];
        let f = c
            .file
            .as_ref()
            .ok_or_else(|| io::Error::other("current binlog segment is not open"))?;
        f.write_all_at(&self.buf, self.buf_start)?;
        #[cfg(test)]
        self.file_ops.push(FileOp::Write {
            seg: c.index,
            off: self.buf_start,
            len: self.buf.len() as u64,
        });
        self.buf_start += self.buf.len() as u64;
        self.buf.clear();
        self.unsynced = true;
        Ok(())
    }

    fn sync_cur(&mut self) -> Result<(), WalError> {
        if let Some(f) = self.segs[self.cur].file.as_ref() {
            sync_fd(f)?;
        }
        self.unsynced = false;
        #[cfg(test)]
        {
            self.sync_count += 1;
            let seg = self.segs[self.cur].index;
            self.file_ops.push(FileOp::Sync { seg });
        }
        Ok(())
    }

    fn seg_pos(&self, index: u64) -> Option<usize> {
        self.segs.binary_search_by_key(&index, |s| s.index).ok()
    }

    fn unanchor(&mut self, seg: u64) {
        if let Some(p) = self.seg_pos(seg) {
            self.segs[p].anchors -= 1;
        }
    }

    /// Record that `id`'s latest Put now lives at (current segment, off).
    fn anchor(&mut self, id: JobId, off: u64) -> u64 {
        let c = &mut self.segs[self.cur];
        c.anchors += 1;
        c.puts.push_back((id, off));
        c.index
    }

    pub(crate) fn append(&mut self, entries: &[JournalEntry]) -> Result<(), WalError> {
        for e in entries {
            match e {
                JournalEntry::Put { record, tube, body } => {
                    let len = put_rec_len(tube.as_str().len(), body.len());
                    if self.pending_puts > 0 {
                        self.pending_puts -= 1;
                        self.pending_bytes = self.pending_bytes.saturating_sub(len);
                    }
                    let off = self.place(len)?;
                    format::encode_put(&mut self.buf, record, tube, body)?;
                    let seg = self.anchor(record.id, off);
                    let old = self.jobs.insert(
                        record.id,
                        JobEntry {
                            record: record.clone(),
                            seg,
                            off,
                            put_len: len,
                            used: len,
                        },
                    );
                    self.live_bytes += len;
                    if let Some(old) = old {
                        self.live_bytes -= old.used;
                        self.unanchor(old.seg);
                    }
                }
                JournalEntry::Update(record) => {
                    self.place(format::UPDATE_REC_LEN)?;
                    format::encode_update(&mut self.buf, record)?;
                    if let Some(j) = self.jobs.get_mut(&record.id) {
                        j.record = record.clone();
                        j.used += format::UPDATE_REC_LEN;
                        self.live_bytes += format::UPDATE_REC_LEN;
                    }
                }
                JournalEntry::Delete(id) => {
                    self.place(DELETE_REC_LEN)?;
                    format::encode_delete(&mut self.buf, *id)?;
                    if let Some(j) = self.jobs.remove(id) {
                        self.live_bytes -= j.used;
                        self.unanchor(j.seg);
                    }
                }
            }
            self.records_written += 1;
        }
        self.pending_puts = 0;
        self.pending_bytes = 0;
        self.flush()?;
        if self.policy == SyncPolicy::Always && self.unsynced {
            self.sync_cur()?;
        }
        Ok(())
    }

    pub(crate) fn sync_if_due(&mut self, now: Instant) -> Result<(), WalError> {
        let SyncPolicy::Interval(every) = self.policy else {
            return Ok(());
        };
        if !self.unsynced {
            return Ok(());
        }
        if let Some(last) = self.last_sync
            && now.saturating_duration_since(last) < every
        {
            return Ok(());
        }
        self.last_sync = Some(now);
        self.sync_cur()
    }

    fn ratio(&self) -> u64 {
        let live = self.live_bytes + self.reserved();
        if live == 0 {
            return 0;
        }
        self.disk_bytes().saturating_sub(live) / live
    }

    pub(crate) fn maintain(&mut self) -> Result<(), WalError> {
        let r = self.ratio();
        if r >= 2 {
            let mut moves = r - 1;
            let mut from = 0;
            while moves > 0 && self.move_one(&mut from)? {
                moves -= 1;
            }
            self.flush()?;
        }
        self.gc()
    }

    /// Move one live job out of the oldest segment that anchors any.
    /// `from` is a search hint (segments before it anchor nothing).
    fn move_one(&mut self, from: &mut usize) -> Result<bool, WalError> {
        // Find the oldest anchoring segment at least two before current.
        let p = loop {
            if *from + 2 > self.cur {
                return Ok(false);
            }
            if self.segs[*from].anchors > 0 {
                break *from;
            }
            *from += 1;
        };
        let seg_index = self.segs[p].index;
        let id = loop {
            let Some(&(id, off)) = self.segs[p].puts.front() else {
                return Err(WalError::Corrupt(format!(
                    "internal: binlog.{seg_index} anchors jobs but lists none"
                )));
            };
            match self.jobs.get(&id) {
                Some(j) if j.seg == seg_index && j.off == off => break id,
                _ => {
                    self.segs[p].puts.pop_front();
                }
            }
        };
        let (off, len) = match self.jobs.get(&id) {
            Some(j) => (j.off, j.put_len),
            None => return Ok(false),
        };
        if !self.ensure_room(len, true) {
            return Ok(false);
        }

        let mut rec = vec![0u8; len as usize];
        self.read_at(seg_index, off, &mut rec)?;
        let record = match format::parse_at(&rec, 0) {
            Parsed::Rec {
                rec: Rec::Put { .. },
                len: l,
            } if l == len => match self.jobs.get(&id) {
                Some(j) => j.record.clone(),
                None => return Ok(false),
            },
            _ => {
                return Err(WalError::Corrupt(format!(
                    "binlog.{seg_index} offset {off}: job {id}'s record is damaged"
                )));
            }
        };
        debug_assert!(rec.len() > JOBREC_OFFSET);
        format::restamp_put(&mut rec, &record);

        let new_off = self.place(len)?;
        self.buf.extend_from_slice(&rec);
        let new_seg = self.anchor(id, new_off);
        self.segs[p].anchors -= 1;
        self.segs[p].puts.pop_front();
        if let Some(j) = self.jobs.get_mut(&id) {
            self.live_bytes = self.live_bytes - j.used + len;
            j.seg = new_seg;
            j.off = new_off;
            j.used = len;
        }
        self.records_written += 1;
        self.records_migrated += 1;
        #[cfg(test)]
        self.moved.insert(id);
        Ok(true)
    }

    fn read_at(&mut self, seg: u64, off: u64, buf: &mut [u8]) -> Result<(), WalError> {
        if self.reader.as_ref().map(|(i, _)| *i) != Some(seg) {
            let p = self
                .seg_pos(seg)
                .ok_or_else(|| io::Error::other("binlog segment vanished"))?;
            self.reader = Some((seg, File::open(&self.segs[p].path)?));
        }
        if let Some((_, f)) = &self.reader {
            f.read_exact_at(buf, off)?;
        }
        Ok(())
    }

    /// Delete segments from the head that anchor no live job.
    fn gc(&mut self) -> Result<(), WalError> {
        let mut removed = false;
        while self.cur > 0 && self.segs[0].anchors == 0 {
            if !removed {
                self.flush()?;
                if self.unsynced && self.policy != SyncPolicy::Never {
                    self.sync_cur()?;
                }
            }
            if let Some(s) = self.segs.pop_front() {
                self.cur -= 1;
                self.disk_total -= s.size;
                if self.reader.as_ref().map(|(i, _)| *i) == Some(s.index) {
                    self.reader = None;
                }
                std::fs::remove_file(&s.path)?;
                removed = true;
            }
        }
        if removed && self.policy == SyncPolicy::Always {
            sync_dir(&self.dir_file)?;
            #[cfg(test)]
            self.file_ops.push(FileOp::DirSync);
        }
        Ok(())
    }

    pub(crate) fn stats(&self) -> BinlogStats {
        BinlogStats {
            oldest_index: self.segs.front().map_or(0, |s| s.index),
            current_index: self.segs[self.cur].index,
            records_written: self.records_written,
            records_migrated: self.records_migrated,
        }
    }
}

fn preallocate(file: &mut File, size: u64) -> io::Result<()> {
    static ZEROS: [u8; 64 * 1024] = [0; 64 * 1024];
    file.write_all(&format::segment_header())?;
    let mut left = size - SEG_HEADER_LEN;
    while left > 0 {
        let n = left.min(ZEROS.len() as u64) as usize;
        file.write_all(&ZEROS[..n])?;
        left -= n as u64;
    }
    Ok(())
}

#[cfg(test)]
impl Inner {
    /// (current segment index, end of written data in it).
    pub(crate) fn cur_pos(&self) -> (u64, u64) {
        let c = &self.segs[self.cur];
        (c.index, c.written)
    }

    pub(crate) fn dir(&self) -> &Path {
        &self.dir
    }

    /// Check the internal bookkeeping against a recomputation.
    pub(crate) fn check_invariants(&self) {
        let live: u64 = self.jobs.values().map(|j| j.used).sum();
        assert_eq!(live, self.live_bytes, "live bytes");
        let disk: u64 = self.segs.iter().map(|s| s.size).sum();
        assert_eq!(disk, self.disk_total, "disk bytes");
        for s in &self.segs {
            assert_eq!(
                std::fs::metadata(&s.path).expect("segment file").len(),
                s.size,
                "size of binlog.{}",
                s.index
            );
        }
        for (p, s) in self.segs.iter().enumerate() {
            let n = self.jobs.values().filter(|j| j.seg == s.index).count() as u64;
            assert_eq!(n, s.anchors, "anchors of binlog.{}", s.index);
            if p > self.cur {
                assert_eq!(s.written, SEG_HEADER_LEN, "spare must be empty");
            }
            assert!(s.written <= s.size);
        }
        for j in self.jobs.values() {
            let p = self.seg_pos(j.seg).expect("anchored segment exists");
            assert!(self.segs[p].puts.contains(&(j.record.id, j.off)));
        }
        assert!(self.buf.is_empty());
    }
}

/// `file_size` rounded up to a whole number of blocks (at least one block),
/// or an error if it exceeds `MAX_SEGMENT_SIZE`.
fn segment_size(file_size: u64) -> Result<u64, WalError> {
    let size = file_size.max(1).div_ceil(BLOCK).checked_mul(BLOCK);
    match size {
        Some(size) if size <= MAX_SEGMENT_SIZE => Ok(size),
        _ => Err(WalError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("binlog file size {file_size} is too large (max {MAX_SEGMENT_SIZE})"),
        ))),
    }
}
