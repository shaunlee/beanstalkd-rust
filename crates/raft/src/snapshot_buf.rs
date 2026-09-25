//! [`SnapshotBuf`]: openraft's `SnapshotData` (an in-memory snapshot being
//! sent or received).
//!
//! openraft 0.9 receives a chunked snapshot by seeking its buffer to each
//! chunk's offset whenever it differs from the end of the previous chunk,
//! then writing the chunk (`Streaming::receive`). With `Cursor<Vec<u8>>`
//! a chunk at a far offset zero-fills the gap, and nothing bounds the
//! total. This buffer instead:
//! - never leaves a gap: seeking beyond the bytes received so far is an
//!   error, so a chunk whose offset is past the current length is rejected
//!   (without allocating);
//! - lets a chunk start at or before the current length: that is the
//!   sender retransmitting a chunk after a timeout (same offset) or
//!   restarting the snapshot from offset 0 with the same id. Writing
//!   discards everything from the write position on, then appends;
//! - refuses to grow beyond a maximum size ([`SnapshotBuf::receiver`]),
//!   and reports failed allocations as errors instead of aborting.
//!
//! A rejected chunk surfaces as a storage error of that `install_snapshot`
//! call on the receiving node (openraft's core is not affected); the
//! leader sees a remote error and retries the snapshot later.

use std::io::{self, SeekFrom};
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, ReadBuf};

/// Default maximum size of a received snapshot: 4 GiB.
///
/// A snapshot is the serialized engine state of the leader, which every
/// node holds in memory anyway, so a legitimate snapshot is bounded by
/// what the leader holds (job bodies dominate). The cap only has to stop a
/// faulty or compromised peer from streaming without end; set too low it
/// would stop a wiped node from ever rejoining, which is worse than the
/// bounded memory use it prevents, so the default is generous. Configure it
/// with [`crate::storage::ClusterStateMachine::set_max_snapshot_bytes`].
pub const DEFAULT_MAX_SNAPSHOT_BYTES: u64 = 4 << 30;

/// An in-memory snapshot: a byte buffer with a position (see the module
/// docs).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SnapshotBuf {
    data: Vec<u8>,
    pos: usize,
    max: usize,
}

impl SnapshotBuf {
    /// An empty buffer for receiving a snapshot of at most `max` bytes.
    pub fn receiver(max: u64) -> Self {
        SnapshotBuf {
            data: Vec::new(),
            pos: 0,
            max: usize::try_from(max).unwrap_or(usize::MAX),
        }
    }

    /// A complete snapshot to read (and send); it cannot grow.
    pub fn from_vec(data: Vec<u8>) -> Self {
        let max = data.len();
        SnapshotBuf { data, pos: 0, max }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.data
    }

    fn write_at_pos(&mut self, buf: &[u8]) -> io::Result<usize> {
        // `pos <= len` always holds (seeks beyond the end are refused).
        self.data.truncate(self.pos);
        let end = self
            .pos
            .checked_add(buf.len())
            .filter(|&e| e <= self.max)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("snapshot exceeds the maximum of {} bytes", self.max),
                )
            })?;
        if end > self.data.capacity() {
            // Grow geometrically, but never past the maximum.
            let want = end
                .max(self.data.capacity().saturating_mul(2))
                .min(self.max);
            self.data
                .try_reserve_exact(want - self.data.len())
                .map_err(|e| io::Error::new(io::ErrorKind::OutOfMemory, e.to_string()))?;
        }
        self.data.extend_from_slice(buf);
        self.pos = end;
        Ok(buf.len())
    }

    fn seek_to(&mut self, pos: SeekFrom) -> io::Result<()> {
        let len = self.data.len() as i128;
        let target = match pos {
            SeekFrom::Start(n) => i128::from(n),
            SeekFrom::End(n) => len + i128::from(n),
            SeekFrom::Current(n) => self.pos as i128 + i128::from(n),
        };
        if !(0..=len).contains(&target) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "snapshot seek to {target} outside the {len} bytes received \
                     (chunks must not leave a gap)"
                ),
            ));
        }
        self.pos = usize::try_from(target).map_err(|_| io::Error::other("seek overflow"))?;
        Ok(())
    }
}

impl AsyncRead for SnapshotBuf {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let rest = this.data.get(this.pos..).unwrap_or(&[]);
        let n = rest.len().min(buf.remaining());
        buf.put_slice(&rest[..n]);
        this.pos += n;
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for SnapshotBuf {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(self.get_mut().write_at_pos(buf))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncSeek for SnapshotBuf {
    fn start_seek(self: Pin<&mut Self>, pos: SeekFrom) -> io::Result<()> {
        self.get_mut().seek_to(pos)
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(self.pos as u64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

    #[tokio::test]
    async fn chunks_in_order_retransmits_and_restarts() {
        let mut b = SnapshotBuf::receiver(100);
        b.write_all(b"hello ").await.expect("chunk 1");
        b.write_all(b"world").await.expect("chunk 2");
        assert_eq!(b.as_slice(), b"hello world");
        // Retransmit of chunk 2 (seek back to its offset).
        b.seek(SeekFrom::Start(6)).await.expect("seek back");
        b.write_all(b"world").await.expect("again");
        assert_eq!(b.as_slice(), b"hello world");
        // Restart from 0 with other data: the old tail is gone.
        b.seek(SeekFrom::Start(0)).await.expect("restart");
        b.write_all(b"abc").await.expect("restart chunk");
        assert_eq!(b.as_slice(), b"abc");
        // Seeking to the end is fine.
        b.seek(SeekFrom::Start(3)).await.expect("end");
        b.write_all(b"d").await.expect("append");
        assert_eq!(b.as_slice(), b"abcd");
    }

    #[tokio::test]
    async fn gaps_are_refused_without_allocating() {
        let mut b = SnapshotBuf::receiver(u64::MAX);
        b.write_all(b"abc").await.expect("chunk");
        let e = b.seek(SeekFrom::Start(1 << 40)).await.expect_err("gap");
        assert_eq!(e.kind(), io::ErrorKind::InvalidInput);
        assert!(b.seek(SeekFrom::Start(4)).await.is_err());
        assert!(b.seek(SeekFrom::Current(-4)).await.is_err());
        assert_eq!(b.as_slice(), b"abc");
        assert!(b.data.capacity() < 1024);
    }

    #[tokio::test]
    async fn the_maximum_is_enforced() {
        let mut b = SnapshotBuf::receiver(8);
        b.write_all(b"12345678").await.expect("exactly the max");
        let e = b.write_all(b"9").await.expect_err("over");
        assert_eq!(e.kind(), io::ErrorKind::InvalidData);
        assert_eq!(b.len(), 8);
        assert!(b.data.capacity() <= 8);
        // A complete snapshot cannot grow.
        let mut s = SnapshotBuf::from_vec(b"xyz".to_vec());
        s.seek(SeekFrom::End(0)).await.expect("end");
        assert!(s.write_all(b"!").await.is_err());
    }

    #[tokio::test]
    async fn reads_from_any_offset() {
        let mut s = SnapshotBuf::from_vec(b"0123456789".to_vec());
        s.seek(SeekFrom::Start(4)).await.expect("seek");
        let mut buf = [0u8; 3];
        s.read_exact(&mut buf).await.expect("read");
        assert_eq!(&buf, b"456");
        let mut rest = Vec::new();
        s.read_to_end(&mut rest).await.expect("rest");
        assert_eq!(rest, b"789");
    }
}
