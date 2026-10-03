//! Directory scan and replay (read-only; `open` applies the fixes).
//!
//! A segment is read until the first position that is not a valid record. A
//! torn segment is accepted only if it is the last one holding records;
//! earlier corruption is `Corrupt`. The rules, and why a torn tail is
//! truncated rather than refused, are in docs/DESIGN.md §7.3.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use bstk_engine::JobRecord;
use bstk_proto::{JobId, TubeName};
use bytes::Bytes;

use crate::WalError;
use crate::format::{MAGIC, Parsed, Rec, SEG_HEADER_LEN, VERSION, parse_at};

/// Parse `binlog.N` strictly: digits only, no leading zeros, N >= 1.
pub(crate) fn segment_index(name: &str) -> Option<u64> {
    let digits = name.strip_prefix("binlog.")?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) || digits.starts_with('0') {
        return None;
    }
    digits.parse().ok().filter(|&n| n >= 1)
}

pub(crate) fn segment_path(dir: &Path, index: u64) -> PathBuf {
    dir.join(format!("binlog.{index}"))
}

#[derive(Debug)]
pub(crate) struct ReplayJob {
    pub record: JobRecord,
    pub tube: TubeName,
    pub body: Bytes,
    /// Sequence number of the record that first established the job.
    pub order: u64,
    /// Location of the job's latest Put record.
    pub seg: u64,
    pub off: u64,
    pub put_len: u64,
    /// Bytes of the latest Put plus the records after it.
    pub used: u64,
}

#[derive(Debug)]
pub(crate) struct ScannedSeg {
    pub index: u64,
    pub path: PathBuf,
    pub file_len: u64,
    /// End of the last valid record.
    pub valid_end: u64,
    pub has_records: bool,
}

#[derive(Debug)]
pub(crate) struct Scan {
    /// Every segment file found, in index order.
    pub segs: Vec<ScannedSeg>,
    pub max_index: u64,
    pub jobs: HashMap<JobId, ReplayJob>,
    pub next_id: JobId,
    pub tube_order: Vec<TubeName>,
    /// (segment index, offset, reason) of the torn tail, if any.
    pub torn: Option<(u64, u64, &'static str)>,
}

pub(crate) fn scan(dir: &Path) -> Result<Scan, WalError> {
    let mut found = Vec::new();
    for e in std::fs::read_dir(dir)? {
        let e = e?;
        if let Some(index) = e.file_name().to_str().and_then(segment_index) {
            found.push(index);
        }
    }
    found.sort_unstable();

    let mut st = ReplayState::default();
    let mut segs = Vec::with_capacity(found.len());
    let mut torn: Option<(u64, u64, &'static str)> = None;

    for &index in &found {
        let path = segment_path(dir, index);
        let buf = std::fs::read(&path)?;
        let file_len = buf.len() as u64;
        let hl = SEG_HEADER_LEN as usize;
        let mut seg = ScannedSeg {
            index,
            path,
            file_len,
            valid_end: 0,
            has_records: false,
        };
        let header_missing = buf.len() < hl || buf[..hl].iter().all(|&b| b == 0);
        if header_missing {
            if buf.iter().any(|&b| b != 0) && torn.is_none() {
                torn = Some((index, SEG_HEADER_LEN, "incomplete segment header"));
            }
            segs.push(seg);
            continue;
        }
        if buf[..8] != MAGIC {
            return Err(corrupt(index, 0, "bad magic"));
        }
        let ver = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);
        if ver != VERSION {
            return Err(WalError::Corrupt(format!(
                "binlog.{index}: unsupported format version {ver}"
            )));
        }

        let mut pos = hl;
        loop {
            match parse_at(&buf, pos) {
                Parsed::End => {
                    if buf[pos..].iter().any(|&b| b != 0) && torn.is_none() {
                        torn = Some((index, pos as u64, "non-zero bytes after end of records"));
                    }
                    break;
                }
                Parsed::Rec { rec, len } => {
                    if let Some((ti, toff, why)) = torn {
                        return Err(WalError::Corrupt(format!(
                            "binlog.{ti} offset {toff}: {why}, but binlog.{index} \
                             offset {pos} holds a later valid record"
                        )));
                    }
                    st.apply(rec, index, pos as u64, len);
                    seg.has_records = true;
                    pos += len as usize;
                }
                Parsed::Malformed(why) => return Err(corrupt(index, pos as u64, why)),
                Parsed::Bad(why) => {
                    if torn.is_none() {
                        torn = Some((index, pos as u64, why));
                    }
                    break;
                }
            }
        }
        seg.valid_end = pos as u64;
        segs.push(seg);
    }

    Ok(Scan {
        max_index: found.last().copied().unwrap_or(0),
        segs,
        next_id: st.max_id.saturating_add(1).max(1),
        jobs: st.jobs,
        tube_order: st.tubes.order,
        torn,
    })
}

fn corrupt(index: u64, off: u64, why: &str) -> WalError {
    WalError::Corrupt(format!("binlog.{index} offset {off}: {why}"))
}

#[derive(Default)]
struct ReplayState {
    jobs: HashMap<JobId, ReplayJob>,
    max_id: JobId,
    seq: u64,
    tubes: TubeList,
}

/// The reference's replay tube list: an `Ms` with swap-removal.
#[derive(Default)]
struct TubeList {
    order: Vec<TubeName>,
    /// Live jobs using the tube, and its position in `order`.
    info: HashMap<TubeName, (u64, usize)>,
}

impl TubeList {
    fn add_job(&mut self, tube: &TubeName) {
        if tube.as_str() == "default" {
            return;
        }
        match self.info.get_mut(tube) {
            Some((refs, _)) => *refs += 1,
            None => {
                self.info.insert(tube.clone(), (1, self.order.len()));
                self.order.push(tube.clone());
            }
        }
    }

    fn remove_job(&mut self, tube: &TubeName) {
        let Some((refs, pos)) = self.info.get_mut(tube) else {
            return;
        };
        *refs -= 1;
        if *refs > 0 {
            return;
        }
        let pos = *pos;
        self.info.remove(tube);
        self.order.swap_remove(pos);
        if let Some(moved) = self.order.get(pos)
            && let Some((_, p)) = self.info.get_mut(moved)
        {
            *p = pos;
        }
    }
}

impl ReplayState {
    fn apply(&mut self, rec: Rec<'_>, seg: u64, off: u64, len: u64) {
        self.max_id = self.max_id.max(rec.id());
        self.seq += 1;
        match rec {
            Rec::Put { record, tube, body } => match self.jobs.get_mut(&record.id) {
                Some(j) => {
                    j.record = record;
                    j.tube = tube;
                    j.body = Bytes::copy_from_slice(body);
                    j.seg = seg;
                    j.off = off;
                    j.put_len = len;
                    j.used = len;
                }
                None => {
                    self.tubes.add_job(&tube);
                    self.jobs.insert(
                        record.id,
                        ReplayJob {
                            record,
                            tube,
                            body: Bytes::copy_from_slice(body),
                            order: self.seq,
                            seg,
                            off,
                            put_len: len,
                            used: len,
                        },
                    );
                }
            },
            Rec::Update(record) => {
                if let Some(j) = self.jobs.get_mut(&record.id) {
                    j.record = record;
                    j.used += len;
                }
            }
            Rec::Delete(id) => {
                if let Some(j) = self.jobs.remove(&id) {
                    self.tubes.remove_job(&j.tube);
                }
            }
        }
    }
}
