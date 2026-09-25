//! Write-ahead log (binlog) for beanstalkd-rs.
//!
//! INTERFACE CONTRACT (owned by the lead): the public API in this file must
//! not change without lead approval. See docs/PLAN.md §4 for the required
//! behavior. The on-disk format is our own; it is not compatible with the
//! reference's binlog.
//!
//! Model (after the reference's walg.c, adapted):
//! - Segment files `binlog.N` in the directory, preallocated to
//!   `file_size` rounded up to a multiple of 4096, plus a `lock` file held
//!   with an exclusive lock for the lifetime of the `Wal`.
//! - Records carry a CRC; replay stops at the first torn or corrupt record
//!   of the last segment and truncates it.
//! - The last record of a job wins; a `Delete` record removes it.
//! - Space: a put may only be accepted (`reserve_put`) while the store can
//!   hold the job's put and delete records and still keep one spare
//!   preallocated segment. Updates never need a reservation: they may use
//!   the spare. Only if even the spare is exhausted does `append` fail.
//! - Compaction (`maintain`): while (allocated - live) / live >= 2, move a
//!   live job out of the oldest segment; delete segments with no live jobs.

use std::path::PathBuf;
use std::time::Duration;

pub use bstk_engine::{BinlogStats, JournalEntry, Recovery};

/// When to fsync, mirroring `-f MS` / `-f0` / `-F`.
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
    _private: (),
}

impl Wal {
    /// Lock the directory (creating it if needed), replay every segment and
    /// start a new current segment. Returns the recovered jobs in replay
    /// order and the next job id.
    pub fn open(opts: WalOptions) -> Result<(Wal, Recovery), WalError> {
        let _ = opts;
        todo!("P1-T1")
    }

    /// Reserve space for a new job's put and delete records. `false` means
    /// the put must be rejected with OUT_OF_MEMORY.
    pub fn reserve_put(&mut self, tube_len: usize, body_len: usize) -> bool {
        let _ = (tube_len, body_len);
        todo!("P1-T1")
    }

    /// Write entries in order. Returns after the bytes reached the OS (and,
    /// with `SyncPolicy::Always`, after fsync). An error is fatal to the
    /// server (fail-stop).
    pub fn append(&mut self, entries: &[JournalEntry]) -> Result<(), WalError> {
        let _ = entries;
        todo!("P1-T1")
    }

    /// With `SyncPolicy::Interval`, fsync if at least one interval has
    /// passed since the last fsync and there are unsynced writes.
    pub fn sync_if_due(&mut self, now: std::time::Instant) -> Result<(), WalError> {
        let _ = now;
        todo!("P1-T1")
    }

    /// Compaction and removal of dead segments; call after `append`.
    pub fn maintain(&mut self) -> Result<(), WalError> {
        todo!("P1-T1")
    }

    pub fn stats(&self) -> BinlogStats {
        todo!("P1-T1")
    }
}
