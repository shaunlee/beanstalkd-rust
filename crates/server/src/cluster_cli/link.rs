//! One admin connection to a node's cluster port: connect, optional mTLS,
//! `AdminHello`, then request/response pairs.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use bstk_raft::NodeId;
use bstk_raft::wire::{
    self, AdminHello, AdminRequest, AdminResponse, ClientMsg, ServerHello, ServerMsg,
};
use rustls_pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::timeout;

/// Largest answer read: a maximal membership is about 270 KiB
/// (`wire::ADMIN_MAX_REQUEST_FRAME`).
const MAX_ANSWER_FRAME: usize = 1 << 20;

/// The SNI of every dial. The node's real name is not known from a bare
/// `HOST:PORT`; `bstk_raft::tls::admin_client_tls_from_pem` takes it from
/// the certificate and the id is checked against the hello afterwards.
const SNI: &str = "bstk-cluster";

/// How the tool authenticates itself and the node.
#[derive(Clone)]
pub enum Auth {
    /// No TLS: the cluster runs `insecure_plaintext` (loopback only).
    Plain,
    Tls(Arc<rustls::ClientConfig>),
}

#[derive(Debug)]
pub enum LinkError {
    /// No connection: unreachable, a TLS failure, or too slow.
    Connect(String),
    /// The node answered the hello with a refusal.
    Rejected(String),
    /// The connection broke or the node spoke out of turn after the hello.
    Io(String),
}

impl fmt::Display for LinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LinkError::Connect(m) | LinkError::Rejected(m) | LinkError::Io(m) => f.write_str(m),
        }
    }
}

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

pub struct Link {
    io: Box<dyn Io>,
    next_id: u64,
    /// The address dialed.
    pub addr: String,
    /// The node id the node announced.
    pub node: NodeId,
}

impl Link {
    /// Opens an admin connection to `addr`, within `budget` for the whole
    /// connect, handshake and hello. `to`: the node id expected there
    /// (checked by the node and against its answer).
    pub async fn open(
        addr: &str,
        auth: &Auth,
        to: Option<NodeId>,
        budget: Duration,
    ) -> Result<Link, LinkError> {
        match timeout(budget, Self::open_inner(addr, auth, to)).await {
            Ok(r) => r,
            Err(_) => Err(LinkError::Connect(format!("{addr}: timed out"))),
        }
    }

    async fn open_inner(addr: &str, auth: &Auth, to: Option<NodeId>) -> Result<Link, LinkError> {
        let tcp = tokio::net::TcpStream::connect(addr)
            .await
            .map_err(|e| LinkError::Connect(format!("{addr}: {e}")))?;
        let _ = tcp.set_nodelay(true);
        let mut certs = None;
        let mut io: Box<dyn Io> = match auth {
            Auth::Plain => Box::new(tcp),
            Auth::Tls(cfg) => {
                let name = ServerName::try_from(SNI)
                    .map_err(|e| LinkError::Connect(format!("{addr}: {e}")))?;
                let tls = tokio_rustls::TlsConnector::from(cfg.clone())
                    .connect(name, tcp)
                    .await
                    .map_err(|e| {
                        LinkError::Connect(format!(
                            "{addr}: TLS handshake failed: {e} (is --ca the cluster CA, and are \
                             --cert and --key the bstk-admin certificate it signed?)"
                        ))
                    })?;
                certs = tls.get_ref().1.peer_certificates().map(<[_]>::to_vec);
                Box::new(tls)
            }
        };
        let hello = ClientMsg::AdminHello(AdminHello {
            version: wire::PROTOCOL_VERSION,
            to,
        });
        let frame = wire::encode(&hello, wire::ADMIN_MAX_REQUEST_FRAME)
            .map_err(|e| LinkError::Connect(format!("{addr}: {e}")))?;
        let gone = |what: String| {
            let hint = match auth {
                // TLS 1.3 reports a client certificate the node refused only
                // when the connection is first used.
                Auth::Tls(_) => {
                    " (the node may have refused the client certificate: is it the \
                                 bstk-admin certificate signed by the cluster CA?)"
                }
                Auth::Plain => {
                    " (plaintext is accepted only by a cluster running insecure_plaintext, from \
                     loopback; an mTLS cluster needs --ca, --cert and --key instead)"
                }
            };
            LinkError::Connect(format!("{addr}: {what}{hint}"))
        };
        wire::write_frame(&mut io, &frame)
            .await
            .map_err(|e| gone(format!("sending the hello failed: {e}")))?;
        let node = match wire::read_frame::<_, ServerMsg>(&mut io, MAX_ANSWER_FRAME).await {
            Ok(Some(ServerMsg::Hello(ServerHello::Accepted {
                version, node_id, ..
            }))) => {
                if version != wire::PROTOCOL_VERSION {
                    return Err(LinkError::Rejected(format!(
                        "{addr}: the node speaks cluster protocol {version}, this tool {}",
                        wire::PROTOCOL_VERSION
                    )));
                }
                node_id
            }
            Ok(Some(ServerMsg::Hello(ServerHello::Rejected { reason }))) => {
                let hint = match auth {
                    Auth::Tls(_) => {
                        " (the cluster accepts only the bstk-admin certificate on this channel, \
                         not a node certificate)"
                    }
                    Auth::Plain => {
                        " (plaintext admin connections are accepted from loopback only, and only \
                         by a cluster running insecure_plaintext)"
                    }
                };
                return Err(LinkError::Rejected(format!(
                    "{addr}: the node refused this client: {reason}{hint}"
                )));
            }
            Ok(Some(other)) => {
                return Err(LinkError::Io(format!(
                    "{addr}: unexpected answer to the hello: {other:?}"
                )));
            }
            Ok(None) => {
                return Err(gone(
                    "the connection was closed before the hello answer".into(),
                ));
            }
            Err(e) => return Err(gone(format!("reading the hello answer failed: {e}"))),
        };
        if let Some(want) = to
            && want != node
        {
            return Err(LinkError::Rejected(format!(
                "{addr}: expected node {want}, found node {node}"
            )));
        }
        if let Auth::Tls(_) = auth {
            bstk_raft::tls::verify_node_certificate(certs.as_deref(), node).map_err(|e| {
                LinkError::Rejected(format!(
                    "{addr}: the node announces id {node} but its certificate does not match: {e}"
                ))
            })?;
        }
        Ok(Link {
            io,
            next_id: 1,
            addr: addr.to_owned(),
            node,
        })
    }

    /// Sends `req` and waits up to `budget` for its answer. After an error
    /// the link is unusable.
    pub async fn call(
        &mut self,
        req: AdminRequest,
        budget: Duration,
    ) -> Result<AdminResponse, LinkError> {
        let id = self.next_id;
        self.next_id += 1;
        let frame = wire::encode(
            &ClientMsg::Admin { id, body: req },
            wire::ADMIN_MAX_REQUEST_FRAME,
        )
        .map_err(|e| LinkError::Io(format!("{}: {e}", self.addr)))?;
        let addr = self.addr.clone();
        let io = &mut self.io;
        let exchange = async {
            wire::write_frame(io, &frame)
                .await
                .map_err(|e| LinkError::Io(format!("{addr}: sending the request failed: {e}")))?;
            match wire::read_frame::<_, ServerMsg>(io, MAX_ANSWER_FRAME).await {
                Ok(Some(ServerMsg::Admin { id: got, body })) if got == id => Ok(body),
                Ok(Some(other)) => Err(LinkError::Io(format!(
                    "{addr}: unexpected message from the node: {other:?}"
                ))),
                Ok(None) => Err(LinkError::Io(format!(
                    "{addr}: the node closed the connection"
                ))),
                Err(e) => Err(LinkError::Io(format!(
                    "{addr}: reading the answer failed: {e}"
                ))),
            }
        };
        match timeout(budget, exchange).await {
            Ok(r) => r,
            Err(_) => Err(LinkError::Io(format!(
                "{}: no answer within {budget:?}",
                self.addr
            ))),
        }
    }
}
