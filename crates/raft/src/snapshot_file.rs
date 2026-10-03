//! [`SnapshotFile`]: openraft's `SnapshotData`, the payload of a snapshot
//! file being sent or received (P4-T5c: snapshots are streamed to and from
//! files, never held in memory; docs/DESIGN.md §8).
//!
//! A `SnapshotFile` is a window `[base, base + len)` of a file in the
//! snapshot directory (the payload of a `.snap` file, see
//! `storage::snapshot`); positions are relative to the window.
//!
//! Sending: openraft reads chunks at increasing offsets (and restarts from
//! 0 after a `SnapshotMismatch`). The checksum of the file is verified as
//! the payload is read in order, so a file whose bytes were damaged fails
//! the read that completes the payload instead of reaching the follower
//! (once failed, the file keeps failing). A file shorter than its layout
//! claims fails with `UnexpectedEof`; the length is checked against the
//! file size only when the store opens, so a file truncated afterwards
//! would otherwise make openraft send empty chunks forever.
//!
//! Receiving: openraft 0.9 seeks to each chunk's offset whenever it
//! differs from the end of the previous chunk, then writes the chunk
//! (`Streaming::receive`). The receiver:
//! - never leaves a gap: seeking beyond the bytes received so far is an
//!   error, so a chunk whose offset is past the current length is rejected
//!   (nothing is written);
//! - lets a chunk start at or before the current length: that is the
//!   sender retransmitting a chunk after a timeout (same offset) or
//!   restarting the snapshot from offset 0 with the same id. Writing
//!   discards everything from the write position on, then appends;
//! - refuses to grow beyond a maximum size ([`DEFAULT_MAX_SNAPSHOT_BYTES`]);
//! - reports a write error at the write that caused it (each write waits
//!   until the file has taken it) and then refuses every write until a
//!   seek, so the file never differs from the chunks accepted;
//! - writes to a temporary file that is removed when the `SnapshotFile` is
//!   dropped (a stream replaced by another snapshot id, a failed install),
//!   unless the install made it the current snapshot; a crash leaves only a
//!   `.tmp` file, removed when the store reopens.
//!
//! A rejected chunk surfaces as a storage error of that `install_snapshot`
//! call on the receiving node (openraft's core is not affected); the
//! leader sees a remote error and retries. openraft keeps the receiving
//! state after a rejected chunk, so the temporary file stays until a
//! different snapshot id arrives, the stream completes or the process
//! exits: at most one file.
//!
//! The file is a `tokio::fs::File`, so chunk reads and writes run on
//! tokio's blocking pool rather than on a runtime worker.

use std::io::{self, SeekFrom};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, AsyncWriteExt, ReadBuf};

/// Default maximum size of a received snapshot payload: 4 GiB.
///
/// A snapshot is the serialized engine state of the leader, which every
/// node holds in memory anyway, so a legitimate snapshot is bounded by
/// what the leader holds (job bodies dominate). The cap only has to stop a
/// faulty or compromised peer from streaming without end; it bounds the
/// disk space of the temporary file in the snapshot directory (a received
/// snapshot is no longer buffered in memory). Set too low it would stop a
/// wiped node from ever rejoining, so the default is generous. Configure
/// it with [`crate::storage::ClusterStateMachine::set_max_snapshot_bytes`].
pub const DEFAULT_MAX_SNAPSHOT_BYTES: u64 = 4 << 30;

/// A file removed on drop unless [`TempPath::keep`] is called.
#[derive(Debug)]
pub(crate) struct TempPath(Option<PathBuf>);

impl TempPath {
    pub(crate) fn new(path: PathBuf) -> Self {
        TempPath(Some(path))
    }

    pub(crate) fn path(&self) -> &Path {
        self.0.as_deref().unwrap_or(Path::new(""))
    }

    /// The file stays (it was renamed into place).
    pub(crate) fn keep(mut self) {
        self.0 = None;
    }
}

impl Drop for TempPath {
    fn drop(&mut self) {
        if let Some(p) = self.0.take()
            && let Err(e) = std::fs::remove_file(&p)
            && e.kind() != io::ErrorKind::NotFound
        {
            tracing::warn!("removing {}: {e}", p.display());
        }
    }
}

/// Checksum verification of a payload read in order (see the module docs).
#[derive(Debug)]
pub(crate) struct CrcCheck {
    /// CRC-32C state after the payload bytes checked so far.
    pub(crate) crc: u32,
    /// Bytes the file's checksum covers after the payload.
    pub(crate) suffix: Vec<u8>,
    pub(crate) expect: u32,
}

/// The payload of a snapshot file (see the module docs).
#[derive(Debug)]
pub struct SnapshotFile {
    file: tokio::fs::File,
    /// File offset of payload byte 0.
    base: u64,
    /// Payload bytes present: the bytes written and kept, or the window of
    /// a file being sent.
    len: u64,
    pos: u64,
    /// Largest length a receiver may reach; `None` for a complete snapshot
    /// (read only).
    max: Option<u64>,
    temp: Option<TempPath>,
    check: Option<CrcCheck>,
    /// Payload bytes covered by `check.crc` (from offset 0).
    checked: u64,
    /// The checksum did not match: every later read fails, so reading the
    /// payload again cannot pass.
    damaged: bool,
    /// Bytes of a write handed to the file and not yet confirmed.
    inflight: Option<usize>,
    /// The file may hold bytes that are not accepted chunks (a write
    /// failed, or is unconfirmed) or lie at an unknown position (a seek
    /// failed): writes fail until a seek succeeds.
    broken: bool,
    /// A seek started by `start_seek` is pending. tokio's `File` tracks
    /// its own position from 0 whatever the position of the `std` file it
    /// wraps, so its `poll_complete` result is used only for our seeks.
    seeking: bool,
}

/// The file behind a [`SnapshotFile`], ready to be decoded.
#[derive(Debug)]
pub(crate) struct Payload {
    pub(crate) file: std::fs::File,
    /// The temporary file of a received snapshot; `None` for a complete
    /// one (openraft's storage test suite installs built snapshots).
    pub(crate) temp: Option<TempPath>,
    pub(crate) base: u64,
    pub(crate) len: u64,
}

fn invalid(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

impl SnapshotFile {
    /// The complete payload `[base, base + len)` of `file`, for sending.
    pub(crate) fn reader(
        mut file: std::fs::File,
        base: u64,
        len: u64,
        check: Option<CrcCheck>,
    ) -> io::Result<Self> {
        io::Seek::seek(&mut file, SeekFrom::Start(base))?;
        Ok(SnapshotFile {
            file: tokio::fs::File::from_std(file),
            base,
            len,
            pos: 0,
            max: None,
            temp: None,
            check,
            checked: 0,
            damaged: false,
            inflight: None,
            broken: false,
            seeking: false,
        })
    }

    /// An empty receiver writing the payload at `base` of the temporary
    /// file `temp` (positioned at `base`), up to `max` bytes.
    pub(crate) fn receiver(file: std::fs::File, temp: TempPath, base: u64, max: u64) -> Self {
        SnapshotFile {
            file: tokio::fs::File::from_std(file),
            base,
            len: 0,
            pos: 0,
            max: Some(max),
            temp: Some(temp),
            check: None,
            checked: 0,
            damaged: false,
            inflight: None,
            broken: false,
            seeking: false,
        }
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Waits for pending writes and hands over the file.
    pub(crate) async fn into_payload(mut self) -> io::Result<Payload> {
        let temp = if self.max.is_some() {
            self.temp.take()
        } else {
            None
        };
        self.file.flush().await?;
        if self.broken {
            return Err(io::Error::other("snapshot file holds a failed write"));
        }
        let file = self.file.into_std().await;
        Ok(Payload {
            file,
            temp,
            base: self.base,
            len: self.len,
        })
    }

    fn verify(&mut self, start: u64, bytes: &[u8]) -> io::Result<()> {
        let Some(c) = self.check.as_mut() else {
            return Ok(());
        };
        if self.damaged {
            return Err(invalid("snapshot file checksum mismatch".into()));
        }
        let end = start + bytes.len() as u64;
        if start > self.checked {
            // openraft only re-reads or restarts from 0, never skips; if
            // it did, the rest cannot be checked.
            self.check = None;
            return Ok(());
        }
        if end > self.checked {
            let skip = (self.checked - start) as usize;
            c.crc = crc32c::crc32c_append(c.crc, &bytes[skip..]);
            self.checked = end;
            if end == self.len && crc32c::crc32c_append(c.crc, &c.suffix) != c.expect {
                self.damaged = true;
                return Err(invalid("snapshot file checksum mismatch".into()));
            }
        }
        Ok(())
    }
}

impl AsyncRead for SnapshotFile {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let rest = this.len.saturating_sub(this.pos);
        if rest == 0 || buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let start = this.pos;
        // Never read past the window: the bytes after the payload (the
        // meta) must stay out of `buf`.
        let want = buf
            .remaining()
            .min(usize::try_from(rest).unwrap_or(usize::MAX));
        let dst = buf.initialize_unfilled_to(want);
        let mut sub = ReadBuf::new(dst);
        ready!(Pin::new(&mut this.file).poll_read(cx, &mut sub))?;
        let n = sub.filled().len();
        if n == 0 {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "snapshot file is shorter than its payload",
            )));
        }
        this.verify(start, &buf.initialized()[buf.filled().len()..][..n])?;
        buf.advance(n);
        this.pos += n as u64;
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for SnapshotFile {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let Some(max) = this.max else {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "a complete snapshot cannot be written",
            )));
        };
        if this
            .pos
            .checked_add(buf.len() as u64)
            .is_none_or(|e| e > max)
        {
            return Poll::Ready(Err(invalid(format!(
                "snapshot exceeds the maximum of {max} bytes"
            ))));
        }
        let n = match this.inflight {
            Some(n) => n,
            None => {
                if this.broken {
                    return Poll::Ready(Err(io::Error::other(
                        "snapshot write after a failed write (seek to resume)",
                    )));
                }
                // Bytes from the write position on are not kept, whatever
                // happens to this write.
                this.len = this.pos;
                let n = match Pin::new(&mut this.file).poll_write(cx, buf) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(n)) => n,
                    Poll::Ready(Err(e)) => {
                        this.broken = true;
                        return Poll::Ready(Err(e));
                    }
                };
                this.broken = true;
                this.inflight = Some(n);
                n
            }
        };
        // tokio's `File` takes a write and performs it in the background,
        // reporting a failure on a later call, by when `pos` would be ahead
        // of the file; waiting here keeps the failure with this write.
        let flushed = ready!(Pin::new(&mut this.file).poll_flush(cx));
        this.inflight = None;
        flushed?;
        this.broken = false;
        this.pos += n as u64;
        this.len = this.pos;
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().file).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().file).poll_shutdown(cx)
    }
}

impl AsyncSeek for SnapshotFile {
    fn start_seek(self: Pin<&mut Self>, pos: SeekFrom) -> io::Result<()> {
        let this = self.get_mut();
        let len = i128::from(this.len);
        let target = match pos {
            SeekFrom::Start(n) => i128::from(n),
            SeekFrom::End(n) => len + i128::from(n),
            SeekFrom::Current(n) => i128::from(this.pos) + i128::from(n),
        };
        if !(0..=len).contains(&target) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "snapshot seek to {target} outside the {len} bytes present \
                     (chunks must not leave a gap)"
                ),
            ));
        }
        let target = u64::try_from(target).map_err(|_| io::Error::other("seek overflow"))?;
        this.inflight = None;
        Pin::new(&mut this.file).start_seek(SeekFrom::Start(this.base + target))?;
        this.seeking = true;
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        let this = self.get_mut();
        let done = ready!(Pin::new(&mut this.file).poll_complete(cx));
        let seeking = std::mem::take(&mut this.seeking);
        let phys = match done {
            Ok(phys) => phys,
            Err(e) => {
                this.broken = true;
                return Poll::Ready(Err(e));
            }
        };
        if seeking {
            this.pos = phys
                .checked_sub(this.base)
                .ok_or_else(|| io::Error::other("snapshot file positioned before its payload"))?;
            // The position is known again, and `len` holds only accepted
            // bytes (see `poll_write`).
            this.broken = false;
        }
        Poll::Ready(Ok(this.pos))
    }
}

/// Test helpers: snapshots in unnamed temporary files (the in-memory test
/// store has no snapshot directory).
#[cfg(test)]
impl SnapshotFile {
    fn temp_file() -> io::Result<(std::fs::File, TempPath)> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "bstk-snapshot-test-{}-{}.tmp",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        Ok((file, TempPath::new(path)))
    }

    /// A complete snapshot holding `bytes`.
    pub(crate) fn from_bytes(bytes: &[u8]) -> io::Result<Self> {
        let (mut file, temp) = Self::temp_file()?;
        io::Write::write_all(&mut file, bytes)?;
        let mut s = Self::reader(file, 0, bytes.len() as u64, None)?;
        s.temp = Some(temp);
        Ok(s)
    }

    /// An empty receiver of at most `max` bytes.
    pub(crate) fn temp_receiver(max: u64) -> io::Result<Self> {
        let (file, temp) = Self::temp_file()?;
        Ok(Self::receiver(file, temp, 0, max))
    }

    /// The whole payload.
    pub(crate) async fn read_all(&mut self) -> io::Result<Vec<u8>> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};
        self.seek(SeekFrom::Start(0)).await?;
        let mut v = Vec::new();
        self.read_to_end(&mut v).await?;
        Ok(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    async fn bytes(b: &mut SnapshotFile) -> Vec<u8> {
        b.read_all().await.expect("read")
    }

    #[tokio::test]
    async fn chunks_in_order_retransmits_and_restarts() {
        let mut b = SnapshotFile::temp_receiver(100).expect("receiver");
        b.write_all(b"hello ").await.expect("chunk 1");
        b.write_all(b"world").await.expect("chunk 2");
        assert_eq!(bytes(&mut b).await, b"hello world");
        // Retransmit of chunk 2 (seek back to its offset).
        b.seek(SeekFrom::Start(6)).await.expect("seek back");
        b.write_all(b"world").await.expect("again");
        assert_eq!(bytes(&mut b).await, b"hello world");
        // Restart from 0 with other data: the old tail is gone.
        b.seek(SeekFrom::Start(0)).await.expect("restart");
        b.write_all(b"abc").await.expect("restart chunk");
        assert_eq!(bytes(&mut b).await, b"abc");
        // Seeking to the end is fine.
        b.seek(SeekFrom::Start(3)).await.expect("end");
        b.write_all(b"d").await.expect("append");
        assert_eq!(bytes(&mut b).await, b"abcd");
        let r = b.into_payload().await.expect("received");
        assert_eq!(r.len, 4);
        assert!(r.temp.is_some());
    }

    #[tokio::test]
    async fn gaps_are_refused_without_writing() {
        let mut b = SnapshotFile::temp_receiver(u64::MAX).expect("receiver");
        b.write_all(b"abc").await.expect("chunk");
        let e = b.seek(SeekFrom::Start(1 << 40)).await.expect_err("gap");
        assert_eq!(e.kind(), io::ErrorKind::InvalidInput);
        assert!(b.seek(SeekFrom::Start(4)).await.is_err());
        assert!(b.seek(SeekFrom::Current(-4)).await.is_err());
        assert_eq!(bytes(&mut b).await, b"abc");
        let path = b.temp.as_ref().expect("temp").path().to_path_buf();
        b.flush().await.expect("flush");
        assert_eq!(std::fs::metadata(&path).expect("file").len(), 3);
    }

    #[tokio::test]
    async fn the_maximum_is_enforced() {
        let mut b = SnapshotFile::temp_receiver(8).expect("receiver");
        b.write_all(b"12345678").await.expect("exactly the max");
        let e = b.write_all(b"9").await.expect_err("over");
        assert_eq!(e.kind(), io::ErrorKind::InvalidData);
        assert_eq!(b.len(), 8);
        // A complete snapshot cannot grow.
        let mut s = SnapshotFile::from_bytes(b"xyz").expect("snapshot");
        s.seek(SeekFrom::End(0)).await.expect("end");
        assert!(s.write_all(b"!").await.is_err());
        assert!(s.into_payload().await.expect("payload").temp.is_none());
    }

    #[tokio::test]
    async fn reads_only_the_window_from_any_offset() {
        let (mut f, temp) = SnapshotFile::temp_file().expect("file");
        io::Write::write_all(&mut f, b"HDR0123456789TRAILER").expect("write");
        let mut s = SnapshotFile::reader(f, 3, 10, None).expect("reader");
        s.seek(SeekFrom::Start(4)).await.expect("seek");
        let mut buf = [0u8; 3];
        s.read_exact(&mut buf).await.expect("read");
        assert_eq!(&buf, b"456");
        let mut rest = Vec::new();
        s.read_to_end(&mut rest).await.expect("rest");
        assert_eq!(rest, b"789");
        assert_eq!(s.seek(SeekFrom::End(0)).await.expect("end"), 10);
        drop(temp);
    }

    #[tokio::test]
    async fn dropping_a_receiver_removes_its_file() {
        let mut b = SnapshotFile::temp_receiver(100).expect("receiver");
        b.write_all(b"partial").await.expect("chunk");
        let path = b.temp.as_ref().expect("temp").path().to_path_buf();
        assert!(path.exists());
        drop(b);
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn a_damaged_payload_fails_the_read_that_completes_it() {
        let payload = b"0123456789".to_vec();
        let suffix = b"meta".to_vec();
        let expect = crc32c::crc32c_append(crc32c::crc32c(&payload), &suffix);
        let check = |expect| CrcCheck {
            crc: 0,
            suffix: suffix.clone(),
            expect,
        };
        let (mut f, temp) = SnapshotFile::temp_file().expect("file");
        io::Write::write_all(&mut f, &payload).expect("write");
        let mut good = SnapshotFile::reader(f, 0, 10, Some(check(expect))).expect("reader");
        // Re-reads and restarts are fine.
        let mut buf = [0u8; 4];
        good.read_exact(&mut buf).await.expect("chunk");
        good.seek(SeekFrom::Start(2)).await.expect("back");
        let mut rest = Vec::new();
        good.read_to_end(&mut rest).await.expect("rest");
        assert_eq!(rest, &payload[2..]);
        drop(temp);

        let (mut f, temp) = SnapshotFile::temp_file().expect("file");
        io::Write::write_all(&mut f, &payload).expect("write");
        let mut bad = SnapshotFile::reader(f, 0, 10, Some(check(expect ^ 1))).expect("reader");
        let mut buf = [0u8; 9];
        bad.read_exact(&mut buf).await.expect("not complete yet");
        let e = bad.read_to_end(&mut Vec::new()).await.expect_err("damaged");
        assert_eq!(e.kind(), io::ErrorKind::InvalidData);
        // Reading it again does not pass.
        bad.seek(SeekFrom::Start(0)).await.expect("restart");
        let e = bad
            .read_to_end(&mut Vec::new())
            .await
            .expect_err("still damaged");
        assert_eq!(e.kind(), io::ErrorKind::InvalidData);
        drop(temp);
    }

    /// A file shorter than its layout is an error, not an endless series of
    /// empty reads.
    #[tokio::test]
    async fn a_truncated_file_fails_the_read() {
        let (mut f, temp) = SnapshotFile::temp_file().expect("file");
        io::Write::write_all(&mut f, b"0123456789").expect("write");
        let mut s = SnapshotFile::reader(f, 0, 20, None).expect("reader");
        let mut buf = [0u8; 8];
        s.read_exact(&mut buf).await.expect("present bytes");
        let e = s.read_exact(&mut buf).await.expect_err("truncated");
        assert_eq!(e.kind(), io::ErrorKind::UnexpectedEof);
        let e = s.read_to_end(&mut Vec::new()).await.expect_err("truncated");
        assert_eq!(e.kind(), io::ErrorKind::UnexpectedEof);
        drop(temp);
    }

    /// A write the file refuses fails at that write (tokio's `File` would
    /// report it on the next call), leaves `pos` and `len` where they were,
    /// and later writes fail until a seek.
    #[tokio::test]
    async fn a_failed_write_is_reported_at_once_and_blocks_later_writes() {
        let (f, temp) = SnapshotFile::temp_file().expect("file");
        let path = temp.path().to_path_buf();
        drop(f);
        // A handle that cannot write.
        let ro = std::fs::File::open(&path).expect("read-only");
        let mut b = SnapshotFile::receiver(ro, temp, 0, 100);
        assert!(b.write_all(b"abc").await.is_err());
        assert_eq!((b.pos, b.len()), (0, 0));
        // Without a seek the next write must not run at an unknown position.
        let e = b.write_all(b"abc").await.expect_err("blocked");
        assert!(e.to_string().contains("failed write"), "{e}");
        assert!(b.into_payload().await.is_err());
    }
}
