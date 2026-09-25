//! A pausable user-space TCP proxy for one directed cluster link.
//!
//! Node A reaches node B's cluster port only through the proxy of the link
//! `A → B` (A's `[[cluster.peer]]` address for B is the proxy). A TCP
//! connection over it carries A's requests ("up") and B's responses
//! ("down"). Faults:
//!
//! - **stall** a direction: stop reading from it; bytes wait in the socket
//!   buffers (and TCP back-pressure stalls the sender), like a black hole
//!   that later heals and delivers everything late;
//! - **sever**: reset every connection and refuse new ones (accept and
//!   close at once), like a hard partition;
//! - **latency**: every chunk waits this long before it is forwarded.
//!
//! [`Proxy::heal`] clears everything; stalled bytes are then delivered, or,
//! with `reset`, the connections are dropped instead.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LinkState {
    pub stall_up: bool,
    pub stall_down: bool,
    pub severed: bool,
    pub latency: Duration,
    /// Incremented to drop every open connection.
    pub epoch: u64,
}

/// One running proxy (dropping it stops accepting; open connections end
/// with the runtime or at the next sever).
pub struct Proxy {
    addr: SocketAddr,
    state: watch::Sender<LinkState>,
    up_bytes: Arc<AtomicU64>,
    down_bytes: Arc<AtomicU64>,
    accept: tokio::task::JoinHandle<()>,
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.accept.abort();
        self.state.send_modify(|s| {
            s.severed = true;
            s.epoch += 1;
        });
    }
}

impl Proxy {
    /// Listens on 127.0.0.1 (a free port) and forwards to `target`.
    pub async fn start(target: SocketAddr) -> io::Result<Proxy> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let addr = listener.local_addr()?;
        let (state, rx) = watch::channel(LinkState::default());
        let up_bytes = Arc::new(AtomicU64::new(0));
        let down_bytes = Arc::new(AtomicU64::new(0));
        let (ub, db) = (up_bytes.clone(), down_bytes.clone());
        let accept = tokio::spawn(async move {
            loop {
                let Ok((client, _)) = listener.accept().await else {
                    continue;
                };
                let st = *rx.borrow();
                if st.severed {
                    drop(client);
                    continue;
                }
                let rx = rx.clone();
                let (ub, db) = (ub.clone(), db.clone());
                tokio::spawn(async move {
                    let Ok(Ok(server)) =
                        tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(target))
                            .await
                    else {
                        return;
                    };
                    let _ = client.set_nodelay(true);
                    let _ = server.set_nodelay(true);
                    let (cr, cw) = client.into_split();
                    let (sr, sw) = server.into_split();
                    let epoch = st.epoch;
                    let up = pump(cr, sw, rx.clone(), epoch, true, ub);
                    let down = pump(sr, cw, rx, epoch, false, db);
                    // Either direction ending closes both.
                    tokio::select! {
                        _ = up => {}
                        _ = down => {}
                    }
                });
            }
        });
        Ok(Proxy {
            addr,
            state,
            up_bytes,
            down_bytes,
            accept,
        })
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn state(&self) -> LinkState {
        *self.state.borrow()
    }

    /// Bytes forwarded (up, down).
    pub fn bytes(&self) -> (u64, u64) {
        (
            self.up_bytes.load(Ordering::Relaxed),
            self.down_bytes.load(Ordering::Relaxed),
        )
    }

    pub fn stall(&self, up: bool, down: bool) {
        self.state.send_modify(|s| {
            s.stall_up |= up;
            s.stall_down |= down;
        });
    }

    pub fn sever(&self) {
        self.state.send_modify(|s| {
            s.severed = true;
            s.epoch += 1;
        });
    }

    pub fn set_latency(&self, d: Duration) {
        self.state.send_modify(|s| s.latency = d);
    }

    /// Clears every fault; with `reset`, open connections are dropped
    /// (stalled bytes are lost) instead of resuming.
    pub fn heal(&self, reset: bool) {
        self.state.send_modify(|s| {
            let epoch = if reset { s.epoch + 1 } else { s.epoch };
            *s = LinkState {
                epoch,
                ..LinkState::default()
            };
        });
    }
}

async fn pump(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    mut rx: watch::Receiver<LinkState>,
    epoch: u64,
    up: bool,
    counter: Arc<AtomicU64>,
) {
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        // Wait while stalled; stop on sever or a new epoch.
        loop {
            let st = *rx.borrow_and_update();
            if st.severed || st.epoch != epoch {
                return;
            }
            let stalled = if up { st.stall_up } else { st.stall_down };
            if !stalled {
                break;
            }
            if rx.changed().await.is_err() {
                return;
            }
        }
        let n = tokio::select! {
            r = from.read(&mut buf) => match r {
                Ok(0) | Err(_) => return,
                Ok(n) => n,
            },
            c = rx.changed() => {
                if c.is_err() {
                    return;
                }
                continue;
            }
        };
        let latency = rx.borrow().latency;
        if !latency.is_zero() {
            tokio::time::sleep(latency).await;
        }
        {
            let st = *rx.borrow();
            if st.severed || st.epoch != epoch {
                return;
            }
        }
        if to.write_all(&buf[..n]).await.is_err() {
            return;
        }
        counter.fetch_add(n as u64, Ordering::Relaxed);
    }
}
