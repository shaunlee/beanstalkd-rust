//! Small durable files the server keeps in `cluster.data_dir`, next to the
//! Raft storage (which ignores them):
//!
//! - [`CONN_IDS_FILE`]: the end of the block of local connection numbers
//!   this node may hand out ([`ConnIdBlocks`]).
//! - [`REJOIN_FILE`]: present while the node is in rejoin mode (see
//!   `super::start`).
//!
//! Every change is durable before it takes effect: write a temporary file,
//! fsync it, rename it over the old one, fsync the directory (and for a
//! removal, fsync the directory after unlinking).

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use bstk_engine::ConnId;
use bstk_raft::{CONN_SEQ_BITS, NodeId, conn_id};

/// Local connection numbers reserved per durable write.
pub const CONN_ID_BLOCK: u64 = 1 << 16;
/// File holding the first local connection number not yet reserved.
pub const CONN_IDS_FILE: &str = "conn-ids";
/// Present while the node is in rejoin mode.
pub const REJOIN_FILE: &str = "rejoin";

/// Replaces `path` with `contents`, durably.
pub fn write_atomic(path: &Path, contents: &[u8]) -> io::Result<()> {
    let dir = parent(path);
    let tmp = path.with_extension("tmp");
    {
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        f.write_all(contents)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    sync_dir(&dir)
}

/// Removes `path` (if present), durably.
pub fn remove_durable(path: &Path) -> io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => sync_dir(&parent(path)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

fn parent(path: &Path) -> PathBuf {
    match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// Whether the rejoin marker is present in `data_dir`.
pub fn rejoin_marked(data_dir: &Path) -> bool {
    data_dir.join(REJOIN_FILE).exists()
}

/// Creates the rejoin marker durably.
pub fn mark_rejoin(data_dir: &Path) -> io::Result<()> {
    write_atomic(
        &data_dir.join(REJOIN_FILE),
        b"rejoining: this node votes only after it has caught up with a leader\n",
    )
}

/// Removes the rejoin marker durably.
pub fn clear_rejoin(data_dir: &Path) -> io::Result<()> {
    remove_durable(&data_dir.join(REJOIN_FILE))
}

/// Reads the persisted end of the reserved block (`None`: no file).
pub fn read_conn_ids(data_dir: &Path) -> Result<Option<u64>, String> {
    let path = data_dir.join(CONN_IDS_FILE);
    match std::fs::read_to_string(&path) {
        Ok(s) => s
            .trim()
            .parse::<u64>()
            .map(Some)
            .map_err(|_| format!("{}: not a connection number: {s:?}", path.display())),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(format!("{}: {e}", path.display())),
    }
}

/// Bits of a local connection number below the seconds of the time floor
/// ([`first_local`]).
pub const TIME_FLOOR_SHIFT: u32 = 16;

/// Without a persisted block (a wiped or new node), the time floor is taken
/// at least this long after the process started ([`first_local`]).
pub const FLOOR_DELAY: std::time::Duration = std::time::Duration::from_secs(1);

/// Seconds since the Unix epoch (0 if the clock is before it).
pub fn unix_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// The first local connection number of a new process:
/// `max(persisted block end, highest number the replicated state has seen
/// for this node + 1, time floor)` with `time floor = unix_seconds << 16`.
///
/// The persisted block end ([`CONN_IDS_FILE`]) covers every number an
/// earlier process may have handed out, but a wiped data directory loses
/// it, and the replicated state only knows the numbers whose `Connect` was
/// committed. The time floor covers the rest: a process that took its
/// floor at `t0` starts at or above `⌊t0⌋ << 16`, so by time `t` it has
/// handed out numbers below `t << 16` as long as the node consumes fewer
/// than 65,536 numbers per second on average (connections, plus one
/// 65,536 block per restart with the file present). A process from a wiped
/// directory takes its floor at `t1` at least [`FLOOR_DELAY`] after it
/// started, hence after the lost process died (at `tc`): `⌊t1⌋ > t1 - 1 >=
/// tc`, so it starts above all of them. Assumptions (docs/DESIGN.md §8):
/// that rate, and a wall clock that is not set back across the restart.
/// The floor must fit in the 48 bits of a local number (until 2106);
/// otherwise this is an error.
pub fn first_local(persisted: Option<u64>, highest: u64, unix_secs: u64) -> Result<u64, String> {
    let floor = unix_secs
        .checked_shl(TIME_FLOOR_SHIFT)
        .filter(|f| f >> TIME_FLOOR_SHIFT == unix_secs && *f < 1 << CONN_SEQ_BITS)
        .ok_or_else(|| {
            format!("the clock ({unix_secs} s since the epoch) does not fit the connection-number time floor")
        })?;
    let first = persisted
        .unwrap_or(0)
        .max(highest.saturating_add(1))
        .max(floor);
    if first >= 1 << CONN_SEQ_BITS {
        return Err("connection numbers of this node are exhausted".into());
    }
    Ok(first)
}

struct Block {
    /// Next local number to hand out.
    next: u64,
    /// End (exclusive) of the durably reserved block.
    limit: u64,
}

/// Hands out this node's local connection numbers in durably reserved
/// blocks: before any number of `[a, a + B)` is used, `a + B` is written
/// to [`CONN_IDS_FILE`], and a new process starts at or above the
/// persisted value. So no number is ever handed out twice, even by a
/// process whose connections never reached the log before it crashed.
pub struct ConnIdBlocks {
    node: NodeId,
    path: PathBuf,
    block: u64,
    state: Mutex<Block>,
}

impl ConnIdBlocks {
    /// Starts handing out numbers at `first` (reserving the first block
    /// durably before returning).
    pub fn open(data_dir: &Path, node: NodeId, first: u64, block: u64) -> Result<Self, String> {
        let this = ConnIdBlocks {
            node,
            path: data_dir.join(CONN_IDS_FILE),
            block: block.max(1),
            state: Mutex::new(Block {
                next: first,
                limit: first,
            }),
        };
        {
            let mut st = this.state.lock().map_err(|_| "poisoned".to_string())?;
            this.reserve(&mut st)?;
        }
        Ok(this)
    }

    fn reserve(&self, st: &mut Block) -> Result<(), String> {
        let limit = st.next.saturating_add(self.block);
        if st.next >= 1 << CONN_SEQ_BITS {
            return Err("connection numbers of this node are exhausted".into());
        }
        write_atomic(&self.path, format!("{limit}\n").as_bytes())
            .map_err(|e| format!("{}: {e}", self.path.display()))?;
        st.limit = limit;
        Ok(())
    }

    /// The next connection id, or `None` if a new block could not be
    /// reserved (the connection must then be refused).
    pub fn next(&self) -> Option<ConnId> {
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if st.next >= st.limit
            && let Err(e) = self.reserve(&mut st)
        {
            tracing::error!("cannot reserve connection numbers: {e}");
            return None;
        }
        let local = st.next;
        st.next += 1;
        Some(conn_id(self.node, local))
    }

    /// The next local number that would be handed out (monitoring).
    pub fn peek_local(&self) -> u64 {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).next
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::cluster::local_of;

    #[test]
    fn blocks_are_reserved_before_use_and_survive_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(read_conn_ids(dir.path()).unwrap(), None);
        let a = ConnIdBlocks::open(dir.path(), 3, 1, 4).unwrap();
        assert_eq!(read_conn_ids(dir.path()).unwrap(), Some(5));
        let ids: Vec<u64> = (0..6).map(|_| local_of(a.next().unwrap())).collect();
        assert_eq!(ids, [1, 2, 3, 4, 5, 6]);
        // The sixth number needed a second block.
        assert_eq!(read_conn_ids(dir.path()).unwrap(), Some(9));
        assert_eq!(bstk_raft::owner_of(a.next().unwrap()), 3);
        drop(a);
        // A "crashed" process used 1..=7; none of them reached the log, so
        // the replicated state knows nothing (highest_local = 0). The next
        // process starts at the persisted end, never below.
        let persisted = read_conn_ids(dir.path()).unwrap().unwrap();
        let first = persisted.max(1);
        let b = ConnIdBlocks::open(dir.path(), 3, first, 4).unwrap();
        assert_eq!(local_of(b.next().unwrap()), 9);
        assert_eq!(b.peek_local(), 10);
        // No temporary file is left behind.
        let names: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, [CONN_IDS_FILE]);
    }

    /// P3-FC: a wiped node loses its persisted block; the time floor still
    /// starts it above every number its lost process could have handed out.
    #[test]
    fn wiped_node_starts_above_the_lost_process() {
        let t0: u64 = 1_790_000_000;
        // First process: empty directory, nothing in the state.
        let dir = tempfile::tempdir().unwrap();
        let first = first_local(None, 0, t0).unwrap();
        assert_eq!(first, t0 << 16);
        let a = ConnIdBlocks::open(dir.path(), 2, first, CONN_ID_BLOCK).unwrap();
        // 10 s at 5,000 connections per second, most of whose `Connect`s
        // never reached the log (the state saw only the first 1,000).
        let mut handed = 0;
        for _ in 0..50_000 {
            handed = local_of(a.next().unwrap());
        }
        let highest_seen = first + 999;
        drop(a);
        // Wiped: the file is gone; the restart 10 s later starts above
        // everything the lost process handed out.
        let wiped = tempfile::tempdir().unwrap();
        let persisted = read_conn_ids(wiped.path()).unwrap();
        assert_eq!(persisted, None);
        let again = first_local(persisted, highest_seen, t0 + 10).unwrap();
        assert!(again > handed, "{again} <= {handed}");
        let b = ConnIdBlocks::open(wiped.path(), 2, again, CONN_ID_BLOCK).unwrap();
        assert!(local_of(b.next().unwrap()) > handed);

        // An ordinary restart keeps the persisted end when it is higher.
        let p = read_conn_ids(dir.path()).unwrap().unwrap();
        assert_eq!(first_local(Some(p), highest_seen, t0 + 1).unwrap(), p);
        // And the state's highest number when that is.
        assert_eq!(first_local(Some(5), 1 << 47, 1).unwrap(), (1 << 47) + 1);
        // The floor must fit 48 bits.
        assert!(first_local(None, 0, 1 << 32).is_err());
        assert!(first_local(None, 0, u64::MAX).is_err());
        assert!(first_local(None, 0, (1 << 32) - 1).is_ok());
    }

    #[test]
    fn unreadable_counter_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(CONN_IDS_FILE), "twelve").unwrap();
        assert!(read_conn_ids(dir.path()).is_err());
    }

    #[test]
    fn rejoin_marker_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!rejoin_marked(dir.path()));
        mark_rejoin(dir.path()).unwrap();
        assert!(rejoin_marked(dir.path()));
        clear_rejoin(dir.path()).unwrap();
        assert!(!rejoin_marked(dir.path()));
        clear_rejoin(dir.path()).unwrap();
    }
}
