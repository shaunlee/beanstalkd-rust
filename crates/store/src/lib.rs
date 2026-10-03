//! Write-ahead log (binlog) for beanstalkd-rs.
//!
//! The public API here is the interface contract with the server: change it
//! only with the lead's approval (docs/PLAN.md §4). The on-disk format is our
//! own, not the reference's binlog format (`format.rs`). Replay and corruption
//! rules: `replay.rs` and docs/DESIGN.md §7.3; space accounting, compaction
//! and crash safety: docs/DESIGN.md §7.1, §7.2.

mod format;
mod replay;
#[cfg(test)]
mod tests;
mod wal;

use std::path::PathBuf;
use std::time::Duration;

pub use bstk_engine::{BinlogStats, JournalEntry, Recovery};

/// Largest accepted `WalOptions::file_size` (4 GiB): room for the largest
/// allowed job (1 GiB) with margin. Segments are preallocated in full, so an
/// absurd `-s` (e.g. a wrapped `-1`) is rejected by `Wal::open` rather than
/// overflowing or filling the disk.
pub const MAX_FILE_SIZE: u64 = 1 << 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPolicy {
    /// `-f0`: fsync after every `append`, before it returns.
    Always,
    /// `-f MS`: fsync at most once per interval (from `sync_if_due`).
    Interval(Duration),
    /// `-F`: never fsync.
    Never,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalOptions {
    pub dir: PathBuf,
    /// `-s`; segment size in bytes (rounded up to a multiple of 4096 on disk).
    pub file_size: u64,
    pub sync: SyncPolicy,
}

#[derive(Debug)]
pub enum WalError {
    /// Another process holds the directory lock (reference exits with 10).
    Locked,
    Io(std::io::Error),
    /// Unrecoverable corruption outside the torn tail of the last segment.
    Corrupt(String),
}

impl std::fmt::Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalError::Locked => f.write_str("binlog directory is locked by another process"),
            WalError::Io(e) => write!(f, "binlog I/O error: {e}"),
            WalError::Corrupt(m) => write!(f, "binlog corrupt: {m}"),
        }
    }
}

impl std::error::Error for WalError {}

impl From<std::io::Error> for WalError {
    fn from(e: std::io::Error) -> Self {
        WalError::Io(e)
    }
}

/// An open binlog. Not thread-safe; owned by the engine actor thread.
#[derive(Debug)]
pub struct Wal {
    inner: wal::Inner,
}

impl Wal {
    /// Lock the directory (creating it if needed), replay every segment and
    /// start a new current segment. Returns the recovered jobs in replay
    /// order and the next job id.
    pub fn open(opts: WalOptions) -> Result<(Wal, Recovery), WalError> {
        let (inner, rec) = wal::Inner::open(opts, None)?;
        Ok((Wal { inner }, rec))
    }

    /// Like `open`, but allocating a segment fails once the segment files
    /// would exceed `max_total_bytes` (simulates a full disk).
    #[cfg(test)]
    pub(crate) fn open_with_limit(
        opts: WalOptions,
        max_total_bytes: u64,
    ) -> Result<(Wal, Recovery), WalError> {
        let (inner, rec) = wal::Inner::open(opts, Some(max_total_bytes))?;
        Ok((Wal { inner }, rec))
    }

    /// Reserve space for a new job's put and delete records. `false` means
    /// the put must be rejected with OUT_OF_MEMORY.
    pub fn reserve_put(&mut self, tube_len: usize, body_len: usize) -> bool {
        self.inner.reserve_put(tube_len, body_len)
    }

    /// Write entries in order. Returns after the bytes reached the OS (and,
    /// with `SyncPolicy::Always`, after fsync). An error is fatal to the
    /// server (fail-stop).
    pub fn append(&mut self, entries: &[JournalEntry]) -> Result<(), WalError> {
        self.inner.append(entries)
    }

    /// With `SyncPolicy::Interval`, fsync if at least one interval has
    /// passed since the last fsync and there are unsynced writes.
    pub fn sync_if_due(&mut self, now: std::time::Instant) -> Result<(), WalError> {
        self.inner.sync_if_due(now)
    }

    pub fn maintain(&mut self) -> Result<(), WalError> {
        self.inner.maintain()
    }

    pub fn stats(&self) -> BinlogStats {
        self.inner.stats()
    }
}
