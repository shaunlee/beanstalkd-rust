//! Durable Raft storage (P3-T2): the log store ([`LogStore`], openraft's
//! `RaftLogStorage`), the state machine wrapping the engine
//! ([`ClusterStateMachine`], openraft's `RaftStateMachine`) and its
//! snapshots.
//!
//! # Data directory layout
//!
//! ```text
//! <data_dir>/log/lock                exclusive lock, held while the LogStore is open
//! <data_dir>/log/<first>.seg         log segments (see `log_store`), 20-digit first index
//! <data_dir>/log/vote                last saved vote (atomic replace)
//! <data_dir>/log/purged              last purged log id (atomic replace)
//! <data_dir>/log/committed           last committed log id (overwritten in place, not synced)
//! <data_dir>/snapshot/lock           exclusive lock, held while the state machine is open
//! <data_dir>/snapshot/<seq>.snap     the current snapshot (older ones are removed)
//! ```
//!
//! Every small file and every log record uses the same framing:
//! `len u32 LE | crc u32 LE (CRC-32C over the 4 len bytes, then the payload) |
//! payload (postcard)`. Temporary files end in `.tmp` and are removed on
//! open.
//!
//! # Durability
//!
//! - `append` writes the whole batch and `fdatasync`s once before calling
//!   the openraft callback (group commit: openraft hands over every entry
//!   queued since the previous call).
//! - `vote`, `purged` and snapshots are written to a temporary file,
//!   `fdatasync`ed, renamed over the old file, and the directory is
//!   `fsync`ed.
//! - `committed` is only a hint that lets a restarted node re-apply
//!   committed entries before it hears from a leader; it is overwritten in
//!   place without a sync, and an unreadable value reads as `None`.
//! - The state machine is not persistent; it is rebuilt from the latest
//!   snapshot and openraft re-applies the log after it.

// openraft's `StorageError` (fixed by its storage traits) is large.
#![allow(clippy::result_large_err)]

mod fsutil;
pub mod log_store;
mod snapshot;
pub mod state_machine;
#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};

pub use log_store::{LogMetrics, LogOptions, LogStore};
pub use state_machine::{
    AppliedInfo, ClusterStateMachine, ReplySink, SmOptions, StateHandle, SysFactory,
};

/// Why a store could not be opened.
#[derive(Debug)]
pub enum OpenError {
    /// Another process holds the directory lock.
    Locked(PathBuf),
    Io(std::io::Error),
    /// Unrecoverable corruption (anything but a torn tail of the last log
    /// segment).
    Corrupt(String),
}

impl std::fmt::Display for OpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OpenError::Locked(p) => {
                write!(f, "{} is locked by another process", p.display())
            }
            OpenError::Io(e) => write!(f, "raft storage I/O error: {e}"),
            OpenError::Corrupt(m) => write!(f, "raft storage corrupt: {m}"),
        }
    }
}

impl std::error::Error for OpenError {}

impl From<std::io::Error> for OpenError {
    fn from(e: std::io::Error) -> Self {
        OpenError::Io(e)
    }
}

/// Subdirectory of the data directory holding the log, vote and purge
/// marker.
pub fn log_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("log")
}

/// Subdirectory of the data directory holding snapshots.
pub fn snapshot_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("snapshot")
}

/// Whether `data_dir` already holds Raft state (a vote, log segments, a
/// purge marker or a snapshot). `--cluster-init` must refuse such a
/// directory.
pub fn has_state(data_dir: &Path) -> std::io::Result<bool> {
    for (dir, is_state) in [
        (
            log_dir(data_dir),
            (|n: &str| n == "vote" || n == "purged" || n.ends_with(".seg")) as fn(&str) -> bool,
        ),
        (snapshot_dir(data_dir), |n: &str| n.ends_with(".snap")),
    ] {
        let rd = match std::fs::read_dir(&dir) {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        for ent in rd {
            let name = ent?.file_name();
            if name.to_str().is_some_and(is_state) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// Open the log store and the state machine of `data_dir`.
pub fn open(
    data_dir: &Path,
    log_opts: LogOptions,
    sm_opts: SmOptions,
) -> Result<(LogStore, ClusterStateMachine), OpenError> {
    let log = LogStore::open(&log_dir(data_dir), log_opts)?;
    let sm = ClusterStateMachine::open(&snapshot_dir(data_dir), sm_opts)?;
    Ok((log, sm))
}
