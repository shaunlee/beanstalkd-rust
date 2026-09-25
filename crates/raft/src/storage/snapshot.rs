//! Snapshot files.
//!
//! # File `<seq>.snap` (seq = 20-digit, increasing per written snapshot)
//!
//! ```text
//! offset 0   magic        8 bytes  b"BSTKSNAP"
//! offset 8   version      u32 LE   1
//! offset 12  crc          u32 LE   CRC-32C of everything from offset 16 on
//! offset 16  meta_len     u64 LE
//! offset 24  payload_len  u64 LE
//! offset 32  meta         postcard(openraft SnapshotMeta)
//!            payload      the snapshot data exactly as sent to other nodes
//!                         (postcard of `state_machine::SnapshotPayload`)
//! ```
//!
//! A snapshot is written to `<seq>.snap.tmp`, fdatasynced, renamed and the
//! directory fsynced; only then are older snapshots removed. On open the
//! newest `.snap` is the current one; leftover temporary files and older
//! snapshots are removed; a damaged newest snapshot refuses to open (it
//! was synced before it was renamed, so damage is not a crash artifact).

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use openraft::{BasicNode, SnapshotMeta};

use super::OpenError;
use super::fsutil;
use crate::NodeId;

const MAGIC: [u8; 8] = *b"BSTKSNAP";
const VERSION: u32 = 1;
const HEADER_LEN: usize = 32;

pub(crate) type Meta = SnapshotMeta<NodeId, BasicNode>;

#[derive(Debug)]
pub(crate) struct SnapshotStore {
    dir: PathBuf,
    _lock: File,
    current: Option<(Meta, PathBuf)>,
    next_seq: u64,
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

fn encode_file(meta: &Meta, payload: &[u8]) -> io::Result<Vec<u8>> {
    let m = postcard::to_allocvec(meta).map_err(|e| invalid(format!("encode: {e}")))?;
    let mut out = Vec::with_capacity(HEADER_LEN + m.len() + payload.len());
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&[0; 4]);
    out.extend_from_slice(&(m.len() as u64).to_le_bytes());
    out.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    out.extend_from_slice(&m);
    out.extend_from_slice(payload);
    let crc = crc32c::crc32c(&out[16..]);
    out[12..16].copy_from_slice(&crc.to_le_bytes());
    Ok(out)
}

/// Read and verify a snapshot file.
fn read_file(path: &Path) -> io::Result<(Meta, Vec<u8>)> {
    let mut f = File::open(path)?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)?;
    let bad = |why: &str| invalid(format!("{}: {why}", path.display()));
    if buf.len() < HEADER_LEN || buf[..8] != MAGIC {
        return Err(bad("bad snapshot header"));
    }
    let word = |r: std::ops::Range<usize>| -> u64 {
        let mut b = [0u8; 8];
        b[..r.len()].copy_from_slice(&buf[r]);
        u64::from_le_bytes(b)
    };
    if word(8..12) != u64::from(VERSION) {
        return Err(bad("unsupported snapshot version"));
    }
    let crc = word(12..16) as u32;
    if crc32c::crc32c(&buf[16..]) != crc {
        return Err(bad("snapshot checksum mismatch"));
    }
    let meta_len = usize::try_from(word(16..24)).map_err(|_| bad("bad length"))?;
    let payload_len = usize::try_from(word(24..32)).map_err(|_| bad("bad length"))?;
    if Some(buf.len())
        != HEADER_LEN
            .checked_add(meta_len)
            .and_then(|n| n.checked_add(payload_len))
    {
        return Err(bad("snapshot length mismatch"));
    }
    let meta: Meta = postcard::from_bytes(&buf[HEADER_LEN..HEADER_LEN + meta_len])
        .map_err(|e| bad(&format!("snapshot meta: {e}")))?;
    buf.drain(..HEADER_LEN + meta_len);
    Ok((meta, buf))
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
            let (meta, _) = read_file(&path).map_err(|e| match e.kind() {
                io::ErrorKind::InvalidData => OpenError::Corrupt(e.to_string()),
                _ => OpenError::Io(e),
            })?;
            current = Some((meta, path));
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
        })
    }

    pub(crate) fn current_meta(&self) -> Option<&Meta> {
        self.current.as_ref().map(|(m, _)| m)
    }

    /// Meta and payload of the current snapshot.
    pub(crate) fn load_current(&self) -> io::Result<Option<(Meta, Vec<u8>)>> {
        match &self.current {
            None => Ok(None),
            Some((_, path)) => read_file(path).map(Some),
        }
    }

    /// A fresh id for a snapshot covering `meta_last`.
    pub(crate) fn new_id(&self, last: &str) -> String {
        format!("{last}-{}", self.next_seq)
    }

    /// Durably store a snapshot and make it the current one. A built
    /// snapshot (`installed == false`) is dropped if the current one
    /// already covers more of the log; an installed one always replaces
    /// it (the state machine now holds its state). Older snapshots are
    /// removed afterwards.
    pub(crate) fn save(&mut self, meta: &Meta, payload: &[u8], installed: bool) -> io::Result<()> {
        if !installed
            && let Some(cur) = self.current_meta()
            && cur.last_log_id > meta.last_log_id
        {
            return Ok(());
        }
        let seq = self.next_seq;
        self.next_seq += 1;
        let bytes = encode_file(meta, payload)?;
        let name = snap_name(seq);
        let tmp = self.dir.join(format!("{name}.tmp"));
        {
            let mut f = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .open(&tmp)?;
            f.write_all(&bytes)?;
            fsutil::sync_data(&f)?;
        }
        #[cfg(test)]
        if crash_point::hit(crash_point::Point::SnapshotBeforeRename) {
            return Err(io::Error::other("injected crash before rename"));
        }
        let path = self.dir.join(&name);
        std::fs::rename(&tmp, &path)?;
        fsutil::sync_dir(&self.dir)?;
        #[cfg(test)]
        if crash_point::hit(crash_point::Point::SnapshotBeforeCleanup) {
            // Durable, but the old snapshot is left behind.
            self.current = Some((meta.clone(), path));
            return Err(io::Error::other("injected crash before cleanup"));
        }
        if let Some((_, old)) = self.current.replace((meta.clone(), path)) {
            std::fs::remove_file(old)?;
        }
        Ok(())
    }
}

/// Test-only crash injection for the snapshot write path.
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
