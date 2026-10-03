//! A TCP stream registered with the runtime for read readiness only
//! (P4-T5b).
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
            match guard.try_io(|s| s.get_ref().read(unfilled)) {
                Ok(Ok(n)) => {
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
}
