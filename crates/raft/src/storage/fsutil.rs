//! File helpers shared by the log store and the snapshot store: syncing,
//! directory locks, atomic file replacement and the record framing.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::Path;

use super::OpenError;

/// Record header: `len u32 | crc u32`.
pub(crate) const REC_HEADER_LEN: u64 = 8;

/// `fdatasync` (plain, not `F_FULLFSYNC` on macOS, like `bstk-store`).
pub(crate) fn sync_data(f: &File) -> io::Result<()> {
    #[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
    {
        nix::unistd::fdatasync(f.as_raw_fd()).map_err(io::Error::from)
    }
    #[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
    {
        nix::unistd::fsync(f.as_raw_fd()).map_err(io::Error::from)
    }
}

/// `fsync` of a directory, so that creations, renames and removals in it
/// are durable.
pub(crate) fn sync_dir(dir: &Path) -> io::Result<()> {
    let f = File::open(dir)?;
    nix::unistd::fsync(f.as_raw_fd()).map_err(io::Error::from)
}

/// Create `dir` if needed and take an exclusive lock on `dir/lock` for as
/// long as the returned file is open.
pub(crate) fn lock_dir(dir: &Path) -> Result<File, OpenError> {
    std::fs::create_dir_all(dir)?;
    let path = dir.join("lock");
    let f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(&path)?;
    match f.try_lock() {
        Ok(()) => Ok(f),
        Err(std::fs::TryLockError::WouldBlock) => Err(OpenError::Locked(dir.to_path_buf())),
        Err(std::fs::TryLockError::Error(e)) => Err(OpenError::Io(e)),
    }
}

/// Remove leftover `*.tmp` files (a crash during an atomic write).
pub(crate) fn remove_tmp_files(dir: &Path) -> io::Result<()> {
    for ent in std::fs::read_dir(dir)? {
        let ent = ent?;
        if ent
            .file_name()
            .to_str()
            .is_some_and(|n| n.ends_with(".tmp"))
        {
            tracing::warn!("removing leftover temporary file {}", ent.path().display());
            std::fs::remove_file(ent.path())?;
        }
    }
    Ok(())
}

/// Replace `dir/name` with `bytes` atomically and durably: write
/// `name.tmp`, fdatasync, rename, fsync the directory.
pub(crate) fn atomic_write(dir: &Path, name: &str, bytes: &[u8]) -> io::Result<()> {
    let tmp = dir.join(format!("{name}.tmp"));
    {
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&tmp)?;
        f.write_all(bytes)?;
        sync_data(&f)?;
    }
    std::fs::rename(&tmp, dir.join(name))?;
    sync_dir(dir)
}

/// Append one framed record holding `payload` to `out`.
pub(crate) fn encode_record(payload: &[u8], out: &mut Vec<u8>) -> io::Result<()> {
    let len = u32::try_from(payload.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "record too large"))?;
    let len_bytes = len.to_le_bytes();
    let mut crc = crc32c::crc32c(&len_bytes);
    crc = crc32c::crc32c_append(crc, payload);
    out.extend_from_slice(&len_bytes);
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(payload);
    Ok(())
}

/// A framed record holding `payload`.
pub(crate) fn record(payload: &[u8]) -> io::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(payload.len() + REC_HEADER_LEN as usize);
    encode_record(payload, &mut out)?;
    Ok(out)
}

/// Why a record could not be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BadRecord {
    /// Fewer bytes than a record header.
    ShortHeader,
    /// A zero length.
    ZeroLength,
    /// The length runs past the end of the data.
    ShortPayload,
    CrcMismatch,
}

impl std::fmt::Display for BadRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            BadRecord::ShortHeader => "truncated record header",
            BadRecord::ZeroLength => "zero record length",
            BadRecord::ShortPayload => "truncated record",
            BadRecord::CrcMismatch => "record checksum mismatch",
        })
    }
}

/// Parse the record at the start of `buf`; returns the payload and the
/// total record length.
pub(crate) fn parse_record(buf: &[u8]) -> Result<(&[u8], usize), BadRecord> {
    let hdr = REC_HEADER_LEN as usize;
    let (Some(len), Some(crc)) = (
        buf.get(0..4)
            .and_then(|b| b.try_into().ok())
            .map(u32::from_le_bytes),
        buf.get(4..8)
            .and_then(|b| b.try_into().ok())
            .map(u32::from_le_bytes),
    ) else {
        return Err(BadRecord::ShortHeader);
    };
    if len == 0 {
        return Err(BadRecord::ZeroLength);
    }
    let len = len as usize;
    let Some(payload) = buf.get(hdr..hdr + len) else {
        return Err(BadRecord::ShortPayload);
    };
    let mut c = crc32c::crc32c(&buf[0..4]);
    c = crc32c::crc32c_append(c, payload);
    if c != crc {
        return Err(BadRecord::CrcMismatch);
    }
    Ok((payload, hdr + len))
}

/// Read the record at `off` in `file` (whose length is `file_len`)
/// without trusting the length field beyond the file size.
pub(crate) fn read_record_at(
    file: &File,
    off: u64,
    file_len: u64,
) -> io::Result<Result<(Vec<u8>, u64), BadRecord>> {
    let avail = file_len.saturating_sub(off);
    if avail < REC_HEADER_LEN {
        return Ok(Err(BadRecord::ShortHeader));
    }
    let mut hdr = [0u8; REC_HEADER_LEN as usize];
    file.read_exact_at(&mut hdr, off)?;
    let len = u64::from(u32::from_le_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]));
    if len == 0 {
        return Ok(Err(BadRecord::ZeroLength));
    }
    if len > avail - REC_HEADER_LEN {
        return Ok(Err(BadRecord::ShortPayload));
    }
    let total = REC_HEADER_LEN + len;
    let mut buf = vec![0u8; total as usize];
    file.read_exact_at(&mut buf, off)?;
    match parse_record(&buf) {
        Ok(_) => {
            buf.drain(..REC_HEADER_LEN as usize);
            Ok(Ok((buf, total)))
        }
        Err(e) => Ok(Err(e)),
    }
}

/// Read a small file holding exactly one record. `Ok(None)` if the file
/// does not exist; `Err` with `InvalidData` if the record is bad.
pub(crate) fn read_record_file(path: &Path) -> io::Result<Option<Vec<u8>>> {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    match parse_record(&bytes) {
        Ok((payload, _)) => Ok(Some(payload.to_vec())),
        Err(why) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{}: {why}", path.display()),
        )),
    }
}
