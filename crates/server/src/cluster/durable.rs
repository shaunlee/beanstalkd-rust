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
