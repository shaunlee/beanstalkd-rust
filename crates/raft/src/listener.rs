//! The accepting side of the cluster port.
//!
//! Per connection: (TLS handshake, which requires a client certificate
//! from the cluster CA), then the dialer's hello within
//! `handshake_timeout`. The hello must name this node as `to`, use this
//! protocol version, and come `from` a configured peer; with TLS the
//! client certificate must also be valid for `bstk-node-<from>`
//! ([`crate::tls::verify_peer_identity`]). Otherwise the listener answers
//! `Rejected` and closes; no request is read before the hello is accepted.
//!
//! Requests on one connection are served strictly in order, one at a time:
//! openraft keeps at most one request in flight per replication stream,
//! and the other users of a link (a vote round, forwarding in the opposite
//! direction of replication) are light, so pipelining buys little and
//! in-order serving keeps per-connection memory to one frame (at most
//! `max_frame` bytes) plus one response. It also preserves the order of
//! forwards from one owner.
//!
//! Inbound connections (including those still in the handshake) are
//! limited to `max_connections`; beyond it new connections are closed at
//! accept.

use std::collections::BTreeSet;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use openraft::Raft;
use rustls::ServerConfig;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::task::{JoinHandle, JoinSet};
use tokio_rustls::TlsAcceptor;

use crate::forward::{ForwardHandler, check_forward};
use crate::wire::{
    self, ClientMsg, FrameError, PROTOCOL_VERSION, RpcRequest, RpcResponse, ServerHello, ServerMsg,
    WireError,
};
use crate::{NodeId, TypeConfig};

/// Settings of the cluster listener.
#[derive(Clone)]
pub struct ListenerConfig {
    /// This node's id; hellos must be addressed to it.
    pub node_id: NodeId,
    /// Node ids allowed to connect (the configured peers).
    pub peers: BTreeSet<NodeId>,
    /// Mutual TLS (see [`crate::tls`]); `None` means plaintext, which the
    /// caller must have been configured for explicitly.
    pub tls: Option<Arc<ServerConfig>>,
    /// Maximum frame payload, both directions.
    pub max_frame: usize,
    /// Inbound connections at once, including handshakes in progress.
    pub max_connections: usize,
    /// TLS handshake plus hello.
    pub handshake_timeout: Duration,
}

impl ListenerConfig {
    /// Defaults: 64 connections, 5 s handshake, 32 MiB frames.
    pub fn new(node_id: NodeId, peers: BTreeSet<NodeId>, tls: Option<Arc<ServerConfig>>) -> Self {
        ListenerConfig {
            node_id,
            peers,
            tls,
            max_frame: wire::DEFAULT_MAX_FRAME,
            max_connections: 64,
            handshake_timeout: Duration::from_secs(5),
        }
    }
}

/// A running cluster listener. Dropping it (or [`shutdown`]) stops
/// accepting and closes every connection it accepted.
///
/// [`shutdown`]: ClusterListener::shutdown
pub struct ClusterListener {
    local_addr: SocketAddr,
    task: Option<JoinHandle<()>>,
}

impl ClusterListener {
    /// Serves `listener`: Raft RPCs go to `raft`, forwards to `handler`.
    pub fn spawn<H: ForwardHandler>(
        listener: TcpListener,
        cfg: ListenerConfig,
        raft: Raft<TypeConfig>,
        handler: Arc<H>,
    ) -> io::Result<Self> {
        let local_addr = listener.local_addr()?;
        let task = tokio::spawn(accept_loop(listener, Arc::new(cfg), raft, handler));
        Ok(ClusterListener {
            local_addr,
            task: Some(task),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Stops accepting, closes all connections and waits for the accept
    /// task to end.
    pub async fn shutdown(mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for ClusterListener {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

async fn accept_loop<H: ForwardHandler>(
    listener: TcpListener,
    cfg: Arc<ListenerConfig>,
    raft: Raft<TypeConfig>,
    handler: Arc<H>,
) {
    let limit = Arc::new(Semaphore::new(cfg.max_connections));
    // Owned here so that aborting this task aborts every connection.
    let mut conns = JoinSet::new();
    loop {
        let accepted = tokio::select! {
            a = listener.accept() => a,
            Some(_) = conns.join_next(), if !conns.is_empty() => continue,
        };
        let (tcp, addr) = match accepted {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!(error = %e, "cluster accept failed");
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let Ok(permit) = limit.clone().try_acquire_owned() else {
            tracing::warn!(%addr, "cluster connection limit reached; closing");
            drop(tcp);
            continue;
        };
        let cfg = cfg.clone();
        let raft = raft.clone();
        let handler = handler.clone();
        conns.spawn(async move {
            if let Err(e) = serve_tcp(tcp, &cfg, &raft, &*handler).await {
                tracing::debug!(%addr, error = %e, "cluster connection closed");
            }
            drop(permit);
        });
    }
}

async fn serve_tcp<H: ForwardHandler>(
    tcp: TcpStream,
    cfg: &ListenerConfig,
    raft: &Raft<TypeConfig>,
    handler: &H,
) -> Result<(), String> {
    let _ = tcp.set_nodelay(true);
    match &cfg.tls {
        None => {
            let mut io = tcp;
            let peer = tokio::time::timeout(cfg.handshake_timeout, hello(&mut io, cfg, |_| Ok(())))
                .await
                .map_err(|_| "hello timed out".to_string())??;
            serve(io, peer, cfg, raft, handler).await
        }
        Some(tls) => {
            let acceptor = TlsAcceptor::from(tls.clone());
            let handshake = async {
                let mut io = acceptor
                    .accept(tcp)
                    .await
                    .map_err(|e| format!("TLS handshake: {e}"))?;
                let certs = io.get_ref().1.peer_certificates().map(<[_]>::to_vec);
                let peer = hello(&mut io, cfg, |id| {
                    crate::tls::verify_peer_identity(certs.as_deref(), id)
                })
                .await?;
                Ok::<_, String>((io, peer))
            };
            let (io, peer) = tokio::time::timeout(cfg.handshake_timeout, handshake)
                .await
                .map_err(|_| "handshake timed out".to_string())??;
            serve(io, peer, cfg, raft, handler).await
        }
    }
}

/// Reads and checks the hello; answers it. Returns the peer's id.
async fn hello<S, F>(io: &mut S, cfg: &ListenerConfig, identity: F) -> Result<NodeId, String>
where
    S: AsyncRead + AsyncWrite + Unpin,
    F: FnOnce(NodeId) -> Result<(), String>,
{
    let h = match wire::read_frame::<_, ClientMsg>(io, cfg.max_frame).await {
        Ok(Some(ClientMsg::Hello(h))) => h,
        Ok(Some(ClientMsg::Request { .. })) => return Err("request before hello".into()),
        Ok(None) => return Err("closed before hello".into()),
        Err(e) => return Err(format!("hello: {e}")),
    };
    let verdict = if h.version != PROTOCOL_VERSION {
        Err(format!("unsupported protocol version {}", h.version))
    } else if h.to != cfg.node_id {
        Err(format!("this is node {}, not {}", cfg.node_id, h.to))
    } else if h.from == cfg.node_id || !cfg.peers.contains(&h.from) {
        Err(format!("node {} is not a configured peer", h.from))
    } else {
        identity(h.from)
    };
    let answer = match &verdict {
        Ok(()) => ServerHello::Accepted {
            version: PROTOCOL_VERSION,
            node_id: cfg.node_id,
        },
        Err(reason) => {
            tracing::warn!(from = h.from, %reason, "cluster hello rejected");
            ServerHello::Rejected {
                reason: reason.clone(),
            }
        }
    };
    let frame =
        wire::encode(&ServerMsg::Hello(answer), cfg.max_frame).map_err(|e| e.to_string())?;
    wire::write_frame(io, &frame)
        .await
        .map_err(|e| e.to_string())?;
    verdict.map(|()| h.from)
}

async fn serve<S, H>(
    mut io: S,
    peer: NodeId,
    cfg: &ListenerConfig,
    raft: &Raft<TypeConfig>,
    handler: &H,
) -> Result<(), String>
where
    S: AsyncRead + AsyncWrite + Unpin,
    H: ForwardHandler,
{
    loop {
        let (id, body) = match wire::read_frame::<_, ClientMsg>(&mut io, cfg.max_frame).await {
            Ok(Some(ClientMsg::Request { id, body })) => (id, body),
            Ok(Some(ClientMsg::Hello(_))) => return Err("second hello".into()),
            Ok(None) => return Ok(()),
            Err(e) => return Err(e.to_string()),
        };
        let resp = dispatch(peer, body, raft, handler).await;
        let frame = match wire::encode(&ServerMsg::Response { id, body: resp }, cfg.max_frame) {
            Ok(f) => f,
            Err(FrameError::TooLarge { len, max }) => {
                return Err(format!("response of {len} bytes exceeds {max}"));
            }
            Err(e) => return Err(e.to_string()),
        };
        wire::write_frame(&mut io, &frame)
            .await
            .map_err(|e| e.to_string())?;
    }
}

/// Serves one request (shared with the simulated network).
pub(crate) async fn dispatch<H: ForwardHandler>(
    peer: NodeId,
    body: RpcRequest,
    raft: &Raft<TypeConfig>,
    handler: &H,
) -> RpcResponse {
    match body {
        RpcRequest::AppendEntries(r) => RpcResponse::AppendEntries(
            raft.append_entries(r)
                .await
                .map_err(|e| WireError::from_raft(&e)),
        ),
        RpcRequest::Vote(r) => {
            RpcResponse::Vote(raft.vote(r).await.map_err(|e| WireError::from_raft(&e)))
        }
        RpcRequest::InstallSnapshot(r) => RpcResponse::InstallSnapshot(
            raft.install_snapshot(r)
                .await
                .map_err(|e| WireError::from_snapshot(&e)),
        ),
        RpcRequest::Forward(r) => RpcResponse::Forward(match check_forward(peer, &r) {
            Ok(()) => Ok(handler.forward(r).await),
            Err(reason) => Err(WireError::Rejected(reason)),
        }),
    }
}
