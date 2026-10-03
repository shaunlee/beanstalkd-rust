//! Snapshot files.
//!
//! # File `<seq>.snap` (seq = 20-digit, increasing per stored snapshot)
//!
//! Version 2 (P4-T5c), written by this version:
//!
//! ```text
//! offset 0   magic        8 bytes  b"BSTKSNAP"
//! offset 8   version      u32 LE   2
//! offset 12  crc          u32 LE   CRC-32C of: payload, then meta, then bytes 16..32
//! offset 16  meta_len     u64 LE
//! offset 24  payload_len  u64 LE
//! offset 32  payload      the snapshot data exactly as sent to other nodes
//!                         (postcard of `state_machine::SnapshotPayload`)
//!            meta         postcard(openraft SnapshotMeta)
//! ```
//!
//! The payload comes first so a snapshot can be streamed into place: a build
//! or a follower writes it into a temporary file after a header placeholder,
//! then appends the meta and writes the header (`commit`); the checksum order
//! lets both compute it in one pass. Version 1 (before P4-T5c; still read,
//! never written): the same header, then meta, then payload, the checksum
//! covering bytes 16.. in file order.
//!
//! A snapshot is written to a `.tmp` file, fdatasynced, renamed and the
//! directory fsynced; only then are older snapshots removed. On open the
//! newest `.snap` is current (its checksum verified by streaming); leftover
//! temporary files and older snapshots are removed; a damaged newest snapshot
//! refuses to open (it was synced before it was renamed, so damage is not a
//! crash artifact).

use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

use openraft::{BasicNode, SnapshotMeta};

use super::OpenError;
use super::fsutil;
use crate::NodeId;
use crate::snapshot_file::{CrcCheck, TempPath};

const MAGIC: [u8; 8] = *b"BSTKSNAP";
const VERSION: u32 = 2;
const VERSION_1: u32 = 1;
/// File offset of the payload in a version 2 file (and of the meta in a
/// version 1 file).
pub(crate) const HEADER_LEN: u64 = 32;
/// Largest meta read from or written to a file: a file whose meta is
/// longer is refused when the store opens, so `commit` must never write one.
/// Peer-supplied parts of the meta are bounded by the wire decoder
/// (`wire::MAX_SNAPSHOT_ID_LEN`, `MAX_NODE_ADDR_LEN`, `MAX_MEMBERS`).
const MAX_META_LEN: u64 = 1 << 20;

// Worst case of a decoded meta (ids, members and addresses at their wire
// limits, with generous room for the varint and log id fields).
const _: () = assert!(
    (crate::wire::MAX_SNAPSHOT_ID_LEN
        + crate::wire::MAX_MEMBERS * (crate::wire::MAX_NODE_ADDR_LEN + 16)
        + crate::wire::MAX_JOINT_CONFIGS * crate::wire::MAX_MEMBERS * 10
        + 256) as u64
        <= MAX_META_LEN
);

pub(crate) type Meta = SnapshotMeta<NodeId, BasicNode>;

#[derive(Debug, Clone)]
pub(crate) struct Layout {
    pub(crate) payload_off: u64,
    pub(crate) payload_len: u64,
    pub(crate) crc_seed: u32,
    pub(crate) crc_suffix: Vec<u8>,
    pub(crate) crc: u32,
}

impl Layout {
    /// Whether `payload_crc` (the CRC-32C state after the payload, from
    /// `crc_seed`) matches the file's checksum.
    pub(crate) fn crc_matches(&self, payload_crc: u32) -> bool {
        crc32c::crc32c_append(payload_crc, &self.crc_suffix) == self.crc
    }

    pub(crate) fn check(&self) -> CrcCheck {
        CrcCheck {
            crc: self.crc_seed,
            suffix: self.crc_suffix.clone(),
            expect: self.crc,
        }
    }
}

#[derive(Debug)]
pub(crate) struct SnapshotStore {
    dir: PathBuf,
    _lock: File,
    current: Option<(Meta, PathBuf, Layout)>,
    next_seq: u64,
    next_tmp: u64,
}

fn snap_name(seq: u64) -> String {
    format!("{seq:020}.snap")
}

fn parse_snap_name(name: &str) -> Option<u64> {
    let digits = name.strip_suffix(".snap")?;
    if digits.len() != 20 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

fn invalid(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

fn lens(meta_len: u64, payload_len: u64) -> [u8; 16] {
    let mut b = [0u8; 16];
    b[..8].copy_from_slice(&meta_len.to_le_bytes());
    b[8..].copy_from_slice(&payload_len.to_le_bytes());
    b
}

/// Reads only the header and meta, never the payload (it can be GiB); the
/// length check against the file size is what stops a truncated file from
/// being trusted.
fn read_layout(f: &File, path: &Path) -> io::Result<(Meta, Layout)> {
    let bad = |why: &str| invalid(format!("{}: {why}", path.display()));
    let size = f.metadata()?.len();
    let mut h = [0u8; HEADER_LEN as usize];
    if size < HEADER_LEN {
        return Err(bad("bad snapshot header"));
    }
    f.read_exact_at(&mut h, 0)?;
    if h[..8] != MAGIC {
        return Err(bad("bad snapshot header"));
    }
    let word = |r: std::ops::Range<usize>| -> u64 {
        let mut b = [0u8; 8];
        b[..r.len()].copy_from_slice(&h[r]);
        u64::from_le_bytes(b)
    };
    let version = word(8..12) as u32;
    let crc = word(12..16) as u32;
    let meta_len = word(16..24);
    let payload_len = word(24..32);
    if meta_len > MAX_META_LEN {
        return Err(bad(&format!(
            "snapshot meta of {meta_len} bytes exceeds the limit of {MAX_META_LEN}"
        )));
    }
    if HEADER_LEN
        .checked_add(meta_len)
        .and_then(|n| n.checked_add(payload_len))
        != Some(size)
    {
        return Err(bad("snapshot length mismatch"));
    }
    let (meta_off, payload_off) = match version {
        VERSION => (HEADER_LEN + payload_len, HEADER_LEN),
        VERSION_1 => (HEADER_LEN, HEADER_LEN + meta_len),
        _ => return Err(bad("unsupported snapshot version")),
    };
    let mut m = vec![0u8; meta_len as usize];
    f.read_exact_at(&mut m, meta_off)?;
    let meta: Meta = postcard::from_bytes(&m).map_err(|e| bad(&format!("snapshot meta: {e}")))?;
    let layout = if version == VERSION {
        m.extend_from_slice(&h[16..32]);
        Layout {
            payload_off,
            payload_len,
            crc_seed: 0,
            crc_suffix: m,
            crc,
        }
    } else {
        Layout {
            payload_off,
            payload_len,
            crc_seed: crc32c::crc32c_append(crc32c::crc32c(&h[16..32]), &m),
            crc_suffix: Vec::new(),
            crc,
        }
    };
    Ok((meta, layout))
}

/// Streams the payload in 1 MiB reads: a snapshot of several GiB is never
/// held in memory.
fn verify(f: &File, path: &Path, layout: &Layout) -> io::Result<()> {
    let mut crc = layout.crc_seed;
    let mut buf = vec![0u8; 1 << 20];
    let mut off = layout.payload_off;
    let end = layout.payload_off + layout.payload_len;
    while off < end {
        let n = buf.len().min((end - off) as usize);
        f.read_exact_at(&mut buf[..n], off)?;
        crc = crc32c::crc32c_append(crc, &buf[..n]);
        off += n as u64;
    }
    if !layout.crc_matches(crc) {
        return Err(invalid(format!(
            "{}: snapshot checksum mismatch",
            path.display()
        )));
    }
    Ok(())
}

impl SnapshotStore {
    pub(crate) fn open(dir: &Path) -> Result<SnapshotStore, OpenError> {
        let lock = fsutil::lock_dir(dir)?;
        fsutil::remove_tmp_files(dir)?;
        let mut seqs = Vec::new();
        for ent in std::fs::read_dir(dir)? {
            if let Some(seq) = ent?.file_name().to_str().and_then(parse_snap_name) {
                seqs.push(seq);
            }
        }
        seqs.sort_unstable();
        let mut current = None;
        if let Some(&newest) = seqs.last() {
            let path = dir.join(snap_name(newest));
            let corrupt = |e: io::Error| match e.kind() {
                io::ErrorKind::InvalidData => OpenError::Corrupt(e.to_string()),
                _ => OpenError::Io(e),
            };
            let f = File::open(&path)?;
            let (meta, layout) = read_layout(&f, &path).map_err(corrupt)?;
            // Before older files are removed below; `restore_current` reads
            // the file once more while decoding it (kept apart so the store
            // does not depend on the decoder).
            verify(&f, &path, &layout).map_err(corrupt)?;
            current = Some((meta, path, layout));
            // A crash after the new snapshot became durable but before the
            // old ones were removed.
            for &old in &seqs[..seqs.len() - 1] {
                std::fs::remove_file(dir.join(snap_name(old)))?;
            }
            if seqs.len() > 1 {
                fsutil::sync_dir(dir)?;
            }
        }
        Ok(SnapshotStore {
            dir: dir.to_path_buf(),
            _lock: lock,
            current,
            next_seq: seqs.last().map_or(1, |s| s + 1),
            next_tmp: 1,
        })
    }

    pub(crate) fn current_meta(&self) -> Option<&Meta> {
        self.current.as_ref().map(|(m, _, _)| m)
    }

    /// Meta, open file and layout of the current snapshot. The file stays
    /// readable when a newer snapshot replaces (and removes) it.
    pub(crate) fn open_current(&self) -> io::Result<Option<(Meta, File, Layout)>> {
        match &self.current {
            None => Ok(None),
            Some((meta, path, layout)) => {
                Ok(Some((meta.clone(), File::open(path)?, layout.clone())))
            }
        }
    }

    /// A fresh id for a snapshot ending at `last`; the sequence number keeps
    /// ids unique when the same log position is snapshotted again.
    pub(crate) fn new_id(&self, last: &str) -> String {
        format!("{last}-{}", self.next_seq)
    }

    /// A new temporary file for a snapshot being built or received,
    /// positioned at the payload (after a zeroed header).
    pub(crate) fn temp_file(&mut self) -> io::Result<(File, TempPath)> {
        let name = format!("{:020}.part.tmp", self.next_tmp);
        self.next_tmp += 1;
        let path = self.dir.join(name);
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)?;
        let temp = TempPath::new(path);
        f.write_all(&[0; HEADER_LEN as usize])?;
        Ok((f, temp))
    }

    /// Durably store the payload `[HEADER_LEN, HEADER_LEN + payload_len)`
    /// of the temporary file `f` (from [`Self::temp_file`]), whose CRC-32C
    /// is `payload_crc`, as a snapshot with `meta`, and make it the current
    /// one. A built snapshot (`installed == false`) is dropped if the
    /// current one already covers more of the log (returns false); an
    /// installed one always replaces it (the state machine now holds its
    /// state). Older snapshots are removed afterwards.
    pub(crate) fn commit(
        &mut self,
        f: &mut File,
        temp: TempPath,
        meta: &Meta,
        payload_len: u64,
        payload_crc: u32,
        installed: bool,
    ) -> io::Result<bool> {
        if !installed
            && let Some(cur) = self.current_meta()
            && cur.last_log_id > meta.last_log_id
        {
            return Ok(false);
        }
        let m = postcard::to_allocvec(meta).map_err(|e| invalid(format!("encode: {e}")))?;
        if m.len() as u64 > MAX_META_LEN {
            // Before anything is written: the current snapshot stays and the
            // temporary file is removed with `temp`.
            return Err(invalid(format!(
                "snapshot meta of {} bytes exceeds the limit of {MAX_META_LEN}",
                m.len()
            )));
        }
        let lens = lens(m.len() as u64, payload_len);
        let crc = crc32c::crc32c_append(crc32c::crc32c_append(payload_crc, &m), &lens);
        let meta_off = HEADER_LEN + payload_len;
        // A received file may hold a longer, abandoned stream after the
        // payload.
        f.set_len(meta_off)?;
        f.seek(SeekFrom::Start(meta_off))?;
        f.write_all(&m)?;
        let mut h = [0u8; HEADER_LEN as usize];
        h[..8].copy_from_slice(&MAGIC);
        h[8..12].copy_from_slice(&VERSION.to_le_bytes());
        h[12..16].copy_from_slice(&crc.to_le_bytes());
        h[16..].copy_from_slice(&lens);
        f.write_all_at(&h, 0)?;
        fsutil::sync_data(f)?;
        #[cfg(test)]
        if crash_point::hit(crash_point::Point::SnapshotBeforeRename) {
            // The temporary file stays, as after a crash.
            temp.keep();
            return Err(io::Error::other("injected crash before rename"));
        }
        let seq = self.next_seq;
        self.next_seq += 1;
        let path = self.dir.join(snap_name(seq));
        std::fs::rename(temp.path(), &path)?;
        temp.keep();
        // From here the new snapshot is the current one whatever else
        // fails: an error would leave `current` stale, and openraft treats
        // an install error as fatal. A restart finds the same state.
        let dir_synced = match fsutil::sync_dir(&self.dir) {
            Ok(()) => true,
            Err(e) => {
                tracing::warn!("syncing {} after storing snapshot: {e}", self.dir.display());
                false
            }
        };
        let mut suffix = m;
        suffix.extend_from_slice(&lens);
        let layout = Layout {
            payload_off: HEADER_LEN,
            payload_len,
            crc_seed: 0,
            crc_suffix: suffix,
            crc,
        };
        #[cfg(test)]
        if crash_point::hit(crash_point::Point::SnapshotBeforeCleanup) {
            // Durable, but the old snapshot is left behind.
            self.current = Some((meta.clone(), path, layout));
            return Err(io::Error::other("injected crash before cleanup"));
        }
        let old = self.current.replace((meta.clone(), path, layout));
        // The old file stays if the rename may not be durable yet; `open`
        // removes older files.
        if dir_synced
            && let Some((_, old, _)) = old
            && let Err(e) = std::fs::remove_file(&old)
        {
            tracing::warn!("removing {}: {e}", old.display());
        }
        Ok(true)
    }
}

/// Writes a version 1 file (the format before P4-T5c), for the test that
/// it is still read.
#[cfg(test)]
pub(crate) fn write_v1(path: &Path, meta: &Meta, payload: &[u8]) -> io::Result<()> {
    let m = postcard::to_allocvec(meta).map_err(|e| invalid(format!("encode: {e}")))?;
    let mut out = Vec::new();
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&VERSION_1.to_le_bytes());
    out.extend_from_slice(&[0; 4]);
    out.extend_from_slice(&lens(m.len() as u64, payload.len() as u64));
    out.extend_from_slice(&m);
    out.extend_from_slice(payload);
    let crc = crc32c::crc32c(&out[16..]);
    out[12..16].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(path, out)
}

/// Production code never holds a payload in memory; tests compare bytes.
#[cfg(test)]
pub(crate) fn read_payload(path: &Path) -> io::Result<Vec<u8>> {
    use std::io::Read;
    let mut f = File::open(path)?;
    let (_, layout) = read_layout(&f, path)?;
    f.seek(SeekFrom::Start(layout.payload_off))?;
    let mut v = Vec::new();
    f.take(layout.payload_len).read_to_end(&mut v)?;
    Ok(v)
}

#[cfg(test)]
pub(crate) mod crash_point {
    use std::cell::Cell;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum Point {
        SnapshotBeforeRename,
        SnapshotBeforeCleanup,
    }

    thread_local! {
        static ARMED: Cell<Option<Point>> = const { Cell::new(None) };
    }

    pub(crate) fn arm(p: Option<Point>) {
        ARMED.with(|a| a.set(p));
    }

    pub(crate) fn hit(p: Point) -> bool {
        ARMED.with(|a| {
            if a.get() == Some(p) {
                a.set(None);
                true
            } else {
                false
            }
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn meta(id: &str) -> Meta {
        Meta {
            snapshot_id: id.into(),
            ..Meta::default()
        }
    }

    fn store(s: &mut SnapshotStore, meta: &Meta, installed: bool) -> io::Result<bool> {
        let (mut f, temp) = s.temp_file()?;
        f.write_all(b"payload")?;
        s.commit(&mut f, temp, meta, 7, crc32c::crc32c(b"payload"), installed)
    }

    fn names(dir: &Path) -> Vec<String> {
        let mut v: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .filter(|n| n != "lock")
            .collect();
        v.sort();
        v
    }

    /// A meta the store would refuse to read back is refused when it is
    /// written, before anything is written and with its own message.
    #[test]
    fn oversized_meta_is_refused_before_it_is_written() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = SnapshotStore::open(dir.path()).unwrap();
        let e = store(&mut s, &meta(&"x".repeat(MAX_META_LEN as usize + 1)), true).unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::InvalidData);
        assert!(e.to_string().contains("meta of"), "{e}");
        assert!(e.to_string().contains("exceeds the limit"), "{e}");
        assert!(names(dir.path()).is_empty(), "{:?}", names(dir.path()));
        assert!(s.current_meta().is_none());

        // An installed snapshot that fails this way leaves the current one.
        store(&mut s, &meta("good"), true).unwrap();
        let files = names(dir.path());
        store(&mut s, &meta(&"x".repeat(MAX_META_LEN as usize + 1)), true).unwrap_err();
        assert_eq!(names(dir.path()), files);
        assert_eq!(s.current_meta().unwrap().snapshot_id, "good");
        drop(s);
        let s = SnapshotStore::open(dir.path()).unwrap();
        assert_eq!(s.current_meta().unwrap().snapshot_id, "good");
    }

    /// A file whose meta is over the limit refuses to open, with the same
    /// message as the write side (not "length mismatch").
    #[test]
    fn oversized_meta_in_a_file_is_reported_as_such() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = SnapshotStore::open(dir.path()).unwrap();
        store(&mut s, &meta("a"), true).unwrap();
        drop(s);
        let path = dir.path().join(snap_name(1));
        let mut b = std::fs::read(&path).unwrap();
        b[16..24].copy_from_slice(&(MAX_META_LEN + 1).to_le_bytes());
        std::fs::write(&path, b).unwrap();
        let e = SnapshotStore::open(dir.path()).unwrap_err();
        assert!(e.to_string().contains("exceeds the limit"), "{e}");
    }

    /// Once the new file is renamed into place it is the current snapshot,
    /// also when removing the old one fails.
    #[test]
    fn a_failed_cleanup_after_the_rename_is_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = SnapshotStore::open(dir.path()).unwrap();
        store(&mut s, &meta("old"), true).unwrap();
        // A directory cannot be removed with `remove_file`.
        let old = dir.path().join(snap_name(1));
        std::fs::remove_file(&old).unwrap();
        std::fs::create_dir(&old).unwrap();
        assert!(store(&mut s, &meta("new"), true).unwrap());
        assert_eq!(s.current_meta().unwrap().snapshot_id, "new");
        assert!(dir.path().join(snap_name(2)).is_file());
    }
}
