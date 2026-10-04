//! Socket helpers shared by the client path (`bstk-server`) and the cluster
//! transport (`bstk-raft`).
//!
//! `QuietTcp` is a TCP stream registered with the runtime for read readiness
//! only (P4-T5b).
//!
//! tokio registers every `TcpStream` for both read and write readiness. On
//! kqueue (macOS, BSD) the write filter fires again whenever the peer
//! acknowledges data, so each frame written to an idle connection costs an
//! extra wake-up of a parked worker thread that finds nothing to do. Here
//! the socket is registered for reads only; a second registration (on a
//! duplicate descriptor) for writes exists only while writes would block.
//! See docs/DESIGN.md §8, "Fewer wake-ups".

use std::io::{self, Read, Write};
use std::net::Shutdown;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncRead, AsyncWrite, Interest, ReadBuf};

pub struct QuietTcp {
    io: AsyncFd<std::net::TcpStream>,
    /// Write-readiness registration, held while writes keep blocking.
    blocked: Option<AsyncFd<std::net::TcpStream>>,
}

impl QuietTcp {
    /// Takes over `tcp` (its options, such as `TCP_NODELAY`, are kept).
    pub fn new(tcp: tokio::net::TcpStream) -> io::Result<QuietTcp> {
        let std = tcp.into_std()?;
        std.set_nonblocking(true)?;
        Ok(QuietTcp {
            io: AsyncFd::with_interest(std, Interest::READABLE)?,
            blocked: None,
        })
    }

    pub fn get_ref(&self) -> &std::net::TcpStream {
        self.io.get_ref()
    }
}

impl std::fmt::Debug for QuietTcp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuietTcp")
            .field("io", self.io.get_ref())
            .field("blocked", &self.blocked.is_some())
            .finish()
    }
}

impl AsyncRead for QuietTcp {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            let mut guard = ready!(this.io.poll_read_ready(cx))?;
            let unfilled = buf.initialize_unfilled();
            let room = unfilled.len();
            match guard.try_io(|s| s.get_ref().read(unfilled)) {
                Ok(Ok(n)) => {
                    // A read that left room in the buffer drained the socket
                    // (epoll and kqueue are edge-triggered), so clear the
                    // readiness now instead of paying a read that fails with
                    // EAGAIN; tokio's own `TcpStream` does the same. The
                    // guard's tick keeps this safe: a newer event, e.g. data
                    // that arrived after the read, is not cleared. A full
                    // buffer may leave more behind, and n == 0 is EOF, which
                    // must stay readable.
                    if 0 < n && n < room {
                        guard.clear_ready();
                    }
                    buf.advance(n);
                    return Poll::Ready(Ok(()));
                }
                Ok(Err(e)) if e.kind() == io::ErrorKind::Interrupted => {}
                Ok(Err(e)) => return Poll::Ready(Err(e)),
                // Would block: readiness was cleared; wait for the next event.
                Err(_) => {}
            }
        }
    }
}

impl AsyncWrite for QuietTcp {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let mut first = true;
        loop {
            match this.io.get_ref().write(buf) {
                Ok(n) => {
                    // Space at the first try: stop watching for it (a
                    // registration left in place wakes the runtime on
                    // every acknowledgement again).
                    if first {
                        this.blocked = None;
                    }
                    return Poll::Ready(Ok(n));
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    let fd = match &mut this.blocked {
                        Some(fd) => fd,
                        // Registering reports the current state, so space
                        // freed since the failed write is not missed.
                        b @ None => b.insert(AsyncFd::with_interest(
                            this.io.get_ref().try_clone()?,
                            Interest::WRITABLE,
                        )?),
                    };
                    ready!(fd.poll_write_ready(cx))?.clear_ready();
                }
                Err(e) => return Poll::Ready(Err(e)),
            }
            first = false;
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(self.io.get_ref().shutdown(Shutdown::Write))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    async fn pair() -> (QuietTcp, TcpStream) {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let (a, b) = tokio::join!(TcpStream::connect(addr), l.accept());
        (QuietTcp::new(a.unwrap()).unwrap(), b.unwrap().0)
    }

    #[tokio::test]
    async fn echoes_and_sees_eof() {
        let (mut q, mut t) = pair().await;
        q.write_all(b"ping").await.unwrap();
        let mut b = [0u8; 4];
        t.read_exact(&mut b).await.unwrap();
        assert_eq!(&b, b"ping");
        t.write_all(b"pong").await.unwrap();
        q.read_exact(&mut b).await.unwrap();
        assert_eq!(&b, b"pong");
        q.shutdown().await.unwrap();
        assert_eq!(t.read(&mut b).await.unwrap(), 0);
        drop(t);
        assert_eq!(q.read(&mut b).await.unwrap(), 0);
    }

    /// Writes far beyond the socket buffers block, then finish once the
    /// peer reads; the data arrives intact and in order, both ways.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn large_writes_block_and_resume() {
        let (q, t) = pair().await;
        let data: Vec<u8> = (0..(8 << 20)).map(|i: u32| (i % 251) as u8).collect();
        let expect = data.clone();
        let (mut qr, mut qw) = tokio::io::split(q);
        let (mut tr, mut tw) = t.into_split();
        let w = tokio::spawn(async move {
            qw.write_all(&data).await.unwrap();
            qw.shutdown().await.unwrap();
        });
        // Let the writer fill the buffers before anyone reads.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let mut got = Vec::new();
        tr.read_to_end(&mut got).await.unwrap();
        w.await.unwrap();
        assert!(got == expect);

        let back = expect.clone();
        let w = tokio::spawn(async move {
            tw.write_all(&back).await.unwrap();
            tw.shutdown().await.unwrap();
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let mut got = Vec::new();
        qr.read_to_end(&mut got).await.unwrap();
        w.await.unwrap();
        assert!(got == expect);
    }

    /// Bursts arrive while the reader is parked, between polls and mid-read;
    /// after every short read the next burst must still be delivered, in
    /// order. A lost wake-up shows up as the timeout.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn short_reads_do_not_lose_later_data() {
        use std::time::Duration;
        const MSGS: u32 = 3000;
        let (mut q, t) = pair().await;
        let (_tr, mut tw) = t.into_split();
        let w = tokio::spawn(async move {
            for i in 0..MSGS {
                // Varying sizes, so reads end both short and full.
                let len = 1 + (i as usize * 37) % 700;
                let msg: Vec<u8> = (0..len).map(|j| (i as usize + j) as u8).collect();
                tw.write_all(&(len as u32).to_be_bytes()).await.unwrap();
                tw.write_all(&msg).await.unwrap();
                match i % 5 {
                    0 => tokio::time::sleep(Duration::from_micros(200)).await,
                    1 => tokio::task::yield_now().await,
                    2 => std::thread::sleep(Duration::from_micros(50)),
                    _ => {}
                }
            }
            tw.shutdown().await.unwrap();
        });
        let r = tokio::time::timeout(Duration::from_secs(60), async move {
            let mut buf = vec![0u8; 64 << 10];
            let mut pending: Vec<u8> = Vec::new();
            let mut done = 0u32;
            loop {
                let n = q.read(&mut buf).await.unwrap();
                if n == 0 {
                    return (done, pending.len());
                }
                pending.extend_from_slice(&buf[..n]);
                while pending.len() >= 4 {
                    let len = u32::from_be_bytes(pending[..4].try_into().unwrap()) as usize;
                    if pending.len() < 4 + len {
                        break;
                    }
                    let i = done as usize;
                    assert_eq!(len, 1 + (i * 37) % 700);
                    assert!(
                        pending[4..4 + len]
                            .iter()
                            .enumerate()
                            .all(|(j, b)| *b == (i + j) as u8)
                    );
                    pending.drain(..4 + len);
                    done += 1;
                }
                // Let the next burst land between this poll and the next.
                match done % 3 {
                    0 => tokio::task::yield_now().await,
                    1 => tokio::time::sleep(Duration::from_micros(100)).await,
                    _ => {}
                }
            }
        })
        .await
        .expect("a wake-up was lost");
        assert_eq!(r, (MSGS, 0));
        w.await.unwrap();
    }

    /// Request and response in lock step: every request is a short read
    /// that clears readiness, and the next one must wake the reader again.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lockstep_requests_each_wake_the_reader() {
        use std::time::Duration;
        let (mut q, mut t) = pair().await;
        let peer = tokio::spawn(async move {
            for i in 0..5000u32 {
                t.write_all(&i.to_be_bytes()).await.unwrap();
                let mut b = [0u8; 4];
                t.read_exact(&mut b).await.unwrap();
                assert_eq!(u32::from_be_bytes(b), i + 1);
            }
        });
        tokio::time::timeout(Duration::from_secs(60), async {
            let mut buf = [0u8; 4096];
            for i in 0..5000u32 {
                let n = q.read(&mut buf).await.unwrap();
                assert_eq!(n, 4);
                assert_eq!(u32::from_be_bytes(buf[..4].try_into().unwrap()), i);
                q.write_all(&(i + 1).to_be_bytes()).await.unwrap();
            }
        })
        .await
        .expect("a wake-up was lost");
        peer.await.unwrap();
    }
}
