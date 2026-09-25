//! On-disk format (version 1). All integers are little-endian.
//!
//! # Segment file `binlog.N`
//!
//! ```text
//! offset 0   magic    8 bytes  b"BSTKWAL\0"
//! offset 8   version  u32      1
//! offset 12  flags    u32      0 (reserved)
//! offset 16  records ...
//!            zero padding up to the preallocated size
//! ```
//!
//! A segment is preallocated by writing zeros (not a sparse `set_len`), so
//! the space is really taken on disk. When a segment stops being the
//! current one it is truncated to the end of its last record.
//!
//! # Record
//!
//! ```text
//! len      u32   length of `payload` (never 0)
//! crc      u32   CRC-32C over the 4 `len` bytes followed by `payload`
//! payload  len bytes:
//!   kind   u8    1 = Put, 2 = Update, 3 = Delete
//!   Put:    JobRecord (57 bytes), tube_len u8, tube bytes, body (rest)
//!   Update: JobRecord (57 bytes)
//!   Delete: job id u64
//! ```
//!
//! JobRecord (57 bytes): id u64, pri u32, delay u32, ttr u32,
//! created_at u64, deadline_at u64, state u8 (0 ready, 1 delayed,
//! 2 buried), reserve_ct u32, timeout_ct u32, release_ct u32, bury_ct u32,
//! kick_ct u32.
//!
//! A `len` of 0 marks the end of the records in a segment (it is what the
//! zero-filled preallocated remainder reads as).

use bstk_engine::{JobRecord, RecordState};
use bstk_proto::{JobId, TubeName};

pub(crate) const MAGIC: [u8; 8] = *b"BSTKWAL\0";
pub(crate) const VERSION: u32 = 1;
pub(crate) const SEG_HEADER_LEN: u64 = 16;
pub(crate) const REC_HEADER_LEN: u64 = 8;
pub(crate) const JOBREC_LEN: usize = 57;

pub(crate) const KIND_PUT: u8 = 1;
pub(crate) const KIND_UPDATE: u8 = 2;
pub(crate) const KIND_DELETE: u8 = 3;

/// Size on disk of a Delete record.
pub(crate) const DELETE_REC_LEN: u64 = REC_HEADER_LEN + 1 + 8;
/// Size on disk of an Update record.
pub(crate) const UPDATE_REC_LEN: u64 = REC_HEADER_LEN + 1 + JOBREC_LEN as u64;
/// Offset of the JobRecord inside a Put or Update record.
pub(crate) const JOBREC_OFFSET: usize = REC_HEADER_LEN as usize + 1;

/// Size on disk of a Put record.
pub(crate) fn put_rec_len(tube_len: usize, body_len: usize) -> u64 {
    REC_HEADER_LEN + 1 + JOBREC_LEN as u64 + 1 + tube_len as u64 + body_len as u64
}

pub(crate) fn segment_header() -> [u8; SEG_HEADER_LEN as usize] {
    let mut h = [0u8; SEG_HEADER_LEN as usize];
    h[..8].copy_from_slice(&MAGIC);
    h[8..12].copy_from_slice(&VERSION.to_le_bytes());
    h
}

fn state_byte(s: RecordState) -> u8 {
    match s {
        RecordState::Ready => 0,
        RecordState::Delayed => 1,
        RecordState::Buried => 2,
    }
}

pub(crate) fn encode_jobrec(r: &JobRecord, out: &mut Vec<u8>) {
    out.extend_from_slice(&r.id.to_le_bytes());
    out.extend_from_slice(&r.pri.to_le_bytes());
    out.extend_from_slice(&r.delay.to_le_bytes());
    out.extend_from_slice(&r.ttr.to_le_bytes());
    out.extend_from_slice(&r.created_at.to_le_bytes());
    out.extend_from_slice(&r.deadline_at.to_le_bytes());
    out.push(state_byte(r.state));
    out.extend_from_slice(&r.reserve_ct.to_le_bytes());
    out.extend_from_slice(&r.timeout_ct.to_le_bytes());
    out.extend_from_slice(&r.release_ct.to_le_bytes());
    out.extend_from_slice(&r.bury_ct.to_le_bytes());
    out.extend_from_slice(&r.kick_ct.to_le_bytes());
}

/// Little-endian cursor over a payload; every getter fails on underrun.
struct Cur<'a>(&'a [u8]);

impl<'a> Cur<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        if self.0.len() < n {
            return None;
        }
        let (a, b) = self.0.split_at(n);
        self.0 = b;
        Some(a)
    }
    fn u8(&mut self) -> Option<u8> {
        self.take(1).map(|b| b[0])
    }
    fn u32(&mut self) -> Option<u32> {
        self.take(4)
            .and_then(|b| b.try_into().ok())
            .map(u32::from_le_bytes)
    }
    fn u64(&mut self) -> Option<u64> {
        self.take(8)
            .and_then(|b| b.try_into().ok())
            .map(u64::from_le_bytes)
    }
}

fn decode_jobrec(c: &mut Cur<'_>) -> Option<JobRecord> {
    let id = c.u64()?;
    let pri = c.u32()?;
    let delay = c.u32()?;
    let ttr = c.u32()?;
    let created_at = c.u64()?;
    let deadline_at = c.u64()?;
    let state = match c.u8()? {
        0 => RecordState::Ready,
        1 => RecordState::Delayed,
        2 => RecordState::Buried,
        _ => return None,
    };
    let reserve_ct = c.u32()?;
    let timeout_ct = c.u32()?;
    let release_ct = c.u32()?;
    let bury_ct = c.u32()?;
    let kick_ct = c.u32()?;
    if id == 0 {
        return None;
    }
    Some(JobRecord {
        id,
        pri,
        delay,
        ttr,
        created_at,
        deadline_at,
        state,
        reserve_ct,
        timeout_ct,
        release_ct,
        bury_ct,
        kick_ct,
    })
}

/// Append a framed record whose payload is produced by `payload`.
fn frame(out: &mut Vec<u8>, payload: impl FnOnce(&mut Vec<u8>)) -> std::io::Result<()> {
    let start = out.len();
    out.extend_from_slice(&[0u8; REC_HEADER_LEN as usize]);
    payload(out);
    let len = u32::try_from(out.len() - start - REC_HEADER_LEN as usize).map_err(|_| {
        out.truncate(start);
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "binlog record too large")
    })?;
    out[start..start + 4].copy_from_slice(&len.to_le_bytes());
    let crc = record_crc(
        &out[start..start + 4],
        &out[start + REC_HEADER_LEN as usize..],
    );
    out[start + 4..start + 8].copy_from_slice(&crc.to_le_bytes());
    Ok(())
}

pub(crate) fn record_crc(len_bytes: &[u8], payload: &[u8]) -> u32 {
    crc32c::crc32c_append(crc32c::crc32c(len_bytes), payload)
}

pub(crate) fn encode_put(
    out: &mut Vec<u8>,
    record: &JobRecord,
    tube: &TubeName,
    body: &[u8],
) -> std::io::Result<()> {
    let tube = tube.as_str().as_bytes();
    let tube_len = u8::try_from(tube.len())
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "tube name too long"))?;
    frame(out, |o| {
        o.push(KIND_PUT);
        encode_jobrec(record, o);
        o.push(tube_len);
        o.extend_from_slice(tube);
        o.extend_from_slice(body);
    })
}

pub(crate) fn encode_update(out: &mut Vec<u8>, record: &JobRecord) -> std::io::Result<()> {
    frame(out, |o| {
        o.push(KIND_UPDATE);
        encode_jobrec(record, o);
    })
}

pub(crate) fn encode_delete(out: &mut Vec<u8>, id: JobId) -> std::io::Result<()> {
    frame(out, |o| {
        o.push(KIND_DELETE);
        o.extend_from_slice(&id.to_le_bytes());
    })
}

/// Re-stamp an encoded Put record (header included) with a new JobRecord
/// and a fresh CRC. Used by compaction moves; the tube and body bytes are
/// kept verbatim.
pub(crate) fn restamp_put(rec: &mut [u8], record: &JobRecord) {
    let mut jr = Vec::with_capacity(JOBREC_LEN);
    encode_jobrec(record, &mut jr);
    rec[JOBREC_OFFSET..JOBREC_OFFSET + JOBREC_LEN].copy_from_slice(&jr);
    let (hdr, payload) = rec.split_at_mut(REC_HEADER_LEN as usize);
    let crc = record_crc(&hdr[..4], payload);
    hdr[4..8].copy_from_slice(&crc.to_le_bytes());
}

/// A decoded record borrowing from the segment buffer.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Rec<'a> {
    Put {
        record: JobRecord,
        tube: TubeName,
        body: &'a [u8],
    },
    Update(JobRecord),
    Delete(JobId),
}

impl Rec<'_> {
    pub(crate) fn id(&self) -> JobId {
        match self {
            Rec::Put { record, .. } | Rec::Update(record) => record.id,
            Rec::Delete(id) => *id,
        }
    }
}

/// Outcome of parsing at one position of a segment buffer.
#[derive(Debug)]
pub(crate) enum Parsed<'a> {
    /// A zero length field, or fewer than 8 bytes left that are all zero.
    End,
    /// A valid record of `len` bytes (header included).
    Rec { rec: Rec<'a>, len: u64 },
    /// A torn or corrupt record (CRC mismatch or past the end of file).
    Bad(&'static str),
    /// The CRC matches but the payload is malformed: never a torn write.
    Malformed(&'static str),
}

pub(crate) fn parse_at(buf: &[u8], pos: usize) -> Parsed<'_> {
    let rest = buf.get(pos..).unwrap_or(&[]);
    if rest.len() < REC_HEADER_LEN as usize {
        if rest.iter().all(|&b| b == 0) {
            return Parsed::End;
        }
        return Parsed::Bad("truncated record header");
    }
    let len_bytes = &rest[..4];
    let len = u32::from_le_bytes([rest[0], rest[1], rest[2], rest[3]]) as usize;
    if len == 0 {
        return Parsed::End;
    }
    let crc = u32::from_le_bytes([rest[4], rest[5], rest[6], rest[7]]);
    let total = REC_HEADER_LEN as usize + len;
    if rest.len() < total {
        return Parsed::Bad("record extends past end of file");
    }
    let payload = &rest[REC_HEADER_LEN as usize..total];
    if record_crc(len_bytes, payload) != crc {
        return Parsed::Bad("CRC mismatch");
    }
    match decode_payload(payload) {
        Ok(rec) => Parsed::Rec {
            rec,
            len: total as u64,
        },
        Err(why) => Parsed::Malformed(why),
    }
}

fn decode_payload(payload: &[u8]) -> Result<Rec<'_>, &'static str> {
    let mut c = Cur(payload);
    let kind = c.u8().ok_or("empty payload")?;
    match kind {
        KIND_PUT => {
            let record = decode_jobrec(&mut c).ok_or("bad job record")?;
            let tl = c.u8().ok_or("missing tube length")? as usize;
            let tube = c.take(tl).ok_or("truncated tube name")?;
            let tube = std::str::from_utf8(tube)
                .ok()
                .and_then(TubeName::new)
                .ok_or("invalid tube name")?;
            Ok(Rec::Put {
                record,
                tube,
                body: c.0,
            })
        }
        KIND_UPDATE => {
            let record = decode_jobrec(&mut c).ok_or("bad job record")?;
            if !c.0.is_empty() {
                return Err("trailing bytes in update record");
            }
            Ok(Rec::Update(record))
        }
        KIND_DELETE => {
            let id = c.u64().ok_or("truncated delete record")?;
            if id == 0 || !c.0.is_empty() {
                return Err("bad delete record");
            }
            Ok(Rec::Delete(id))
        }
        _ => Err("unknown record kind"),
    }
}
