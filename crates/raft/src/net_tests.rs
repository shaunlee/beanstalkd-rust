//! Tests of the TCP / TLS transport over loopback.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bstk_engine::EngineInput;
use openraft::error::{InstallSnapshotError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, VoteRequest,
};
use openraft::{
    BasicNode, CommittedLeaderId, Entry, EntryPayload, LogId, Membership, Raft, SnapshotMeta,
    StoredMembership, Vote,
};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use rustls_pki_types::pem::PemObject;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::client::{Network, NetworkConfig, PeerClient};
use crate::forward::{
    ControlRequest, ControlResponse, ForwardError, ForwardHandler, ForwardTransport,
};
use crate::listener::{ClusterListener, ListenerConfig, VoteGate};
use crate::test_store::{MemLog, MemSm};
use crate::tls::{ClusterTls, cluster_tls_from_pem, node_dns_name};
use crate::wire::{self, ClientMsg, Hello, PROTOCOL_VERSION, ServerHello, ServerMsg};
use crate::{ForwardRequest, ForwardResponse, NodeId, Op, Request, TypeConfig, conn_id};

// ---------------------------------------------------------------- helpers

#[derive(Default)]
struct CountingHandler {
    calls: AtomicUsize,
    last: Mutex<Option<ForwardRequest>>,
    controls: Mutex<Vec<ControlRequest>>,
}

impl CountingHandler {
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl ForwardHandler for CountingHandler {
    async fn forward(&self, req: ForwardRequest) -> ForwardResponse {
        self.calls.fetch_add(1, Ordering::SeqCst);
        *self.last.lock().expect("lock") = Some(req);
        ForwardResponse::NotLeader { leader: Some(3) }
    }

    async fn control(&self, req: ControlRequest) -> ControlResponse {
        self.controls.lock().expect("lock").push(req);
        ControlResponse::Accepted { index: Some(7) }
    }
}

fn raft_config() -> Arc<openraft::Config> {
    let c = openraft::Config {
        heartbeat_interval: 50,
        election_timeout_min: 150,
        election_timeout_max: 300,
        ..Default::default()
    };
    Arc::new(c.validate().expect("valid config"))
}

fn net_config(
    id: NodeId,
    peers: BTreeMap<NodeId, String>,
    tls: Option<&ClusterTls>,
) -> NetworkConfig {
    let mut c = NetworkConfig::new(id, peers, tls.map(|t| t.client.clone()));
    c.connect_timeout = Duration::from_millis(500);
    c.append_timeout = Duration::from_millis(500);
    c.vote_timeout = Duration::from_millis(500);
    c.forward_timeout = Duration::from_millis(500);
    c.backoff_min = Duration::from_millis(10);
    c.backoff_max = Duration::from_millis(100);
    c
}

fn option() -> RPCOption {
    RPCOption::new(Duration::from_secs(5))
}

fn req(now: u64) -> Request {
    Request { now, op: Op::Tick }
}

/// A CA and per-node certificates (SAN `bstk-node-<id>`).
struct Pki {
    ca: rcgen::Certificate,
    ca_key: KeyPair,
}

impl Pki {
    fn new(name: &str) -> Self {
        let mut params = CertificateParams::new(Vec::<String>::new()).expect("params");
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.distinguished_name.push(DnType::CommonName, name);
        params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let ca_key = KeyPair::generate().expect("key");
        let ca = params.self_signed(&ca_key).expect("ca");
        Pki { ca, ca_key }
    }

    fn ca_pem(&self) -> String {
        self.ca.pem()
    }

    /// `(cert PEM, key PEM)` for SAN DNS names `names`.
    fn leaf(&self, names: &[String]) -> (String, String) {
        let mut params = CertificateParams::new(names.to_vec()).expect("params");
        params
            .distinguished_name
            .push(DnType::CommonName, "bstk test node");
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
        ];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let key = KeyPair::generate().expect("key");
        let cert = params
            .signed_by(&key, &self.ca, &self.ca_key)
            .expect("sign");
        (cert.pem(), key.serialize_pem())
    }

    fn node(&self, id: NodeId) -> ClusterTls {
        let (cert, key) = self.leaf(&[node_dns_name(id)]);
        cluster_tls_from_pem(
            id,
            cert.as_bytes(),
            key.as_bytes(),
            self.ca_pem().as_bytes(),
        )
        .expect("tls config")
    }
}

/// TLS material with a certificate for `cert_id` signed by `signer`,
/// trusting `trust`'s CA (used by a node with a different id).
fn tls_with_cert_for(signer: &Pki, trust: &Pki, cert_id: NodeId) -> ClusterTls {
    let (cert, key) = signer.leaf(&[node_dns_name(cert_id)]);
    cluster_tls_from_pem(
        cert_id,
        cert.as_bytes(),
        key.as_bytes(),
        trust.ca_pem().as_bytes(),
    )
    .expect("tls config")
}

/// One Raft node with a cluster listener.
struct Node {
    id: NodeId,
    raft: Raft<TypeConfig>,
    sm: MemSm,
    handler: Arc<CountingHandler>,
    listener: Option<ClusterListener>,
    addr: SocketAddr,
}

async fn bind() -> TcpListener {
    TcpListener::bind("127.0.0.1:0").await.expect("bind")
}

fn listener_config(id: NodeId, peers: &[NodeId], tls: Option<&ClusterTls>) -> ListenerConfig {
    let mut c = ListenerConfig::new(
        id,
        peers.iter().copied().collect::<BTreeSet<_>>(),
        tls.map(|t| t.server.clone()),
    );
    c.handshake_timeout = Duration::from_secs(2);
    c
}

async fn start_node(
    id: NodeId,
    tcp: TcpListener,
    addrs: &BTreeMap<NodeId, String>,
    tls: Option<&ClusterTls>,
    config: Arc<openraft::Config>,
) -> Node {
    let addr = tcp.local_addr().expect("addr");
    let net = Network::new(net_config(id, addrs.clone(), tls));
    let sm = MemSm::default();
    let raft = Raft::new(id, config, net.clone(), MemLog::default(), sm.clone())
        .await
        .expect("raft");
    let handler = Arc::new(CountingHandler::default());
    let ids: Vec<NodeId> = addrs.keys().copied().collect();
    let listener = ClusterListener::spawn(
        tcp,
        listener_config(id, &ids, tls),
        raft.clone(),
        handler.clone(),
    )
    .expect("listener");
    Node {
        id,
        raft,
        sm,
        handler,
        listener: Some(listener),
        addr,
    }
}

async fn start_cluster(n: u64, pki: Option<&Pki>, config: Arc<openraft::Config>) -> Vec<Node> {
    let mut tcps = Vec::new();
    let mut addrs = BTreeMap::new();
    for id in 1..=n {
        let t = bind().await;
        addrs.insert(id, t.local_addr().expect("addr").to_string());
        tcps.push((id, t));
    }
    let mut nodes = Vec::new();
    for (id, t) in tcps {
        let tls = pki.map(|p| p.node(id));
        nodes.push(start_node(id, t, &addrs, tls.as_ref(), config.clone()).await);
    }
    let members: BTreeMap<NodeId, BasicNode> = addrs
        .iter()
        .map(|(id, a)| (*id, BasicNode::new(a.clone())))
        .collect();
    nodes[0].raft.initialize(members).await.expect("initialize");
    nodes
}

async fn wait_leader(nodes: &[Node]) -> NodeId {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let leaders: Vec<_> = nodes
            .iter()
            .map(|n| n.raft.metrics().borrow().current_leader)
            .collect();
        if let Some(Some(l)) = leaders.first()
            && leaders.iter().all(|x| *x == Some(*l))
        {
            return *l;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no leader: {leaders:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn replicate_and_check(nodes: &[Node], from: u64, count: u64) {
    let leader = wait_leader(nodes).await;
    let l = nodes.iter().find(|n| n.id == leader).expect("leader node");
    let mut last = 0;
    for i in from..from + count {
        let r = l.raft.client_write(req(i)).await.expect("client_write");
        last = r.log_id.index;
    }
    for n in nodes {
        n.raft
            .wait(Some(Duration::from_secs(10)))
            .applied_index_at_least(Some(last), "replicated")
            .await
            .expect("applied");
    }
    let want = l.sm.applied();
    for n in nodes {
        assert_eq!(n.sm.applied(), want, "node {}", n.id);
    }
}

async fn shutdown(nodes: Vec<Node>) {
    for mut n in nodes {
        if let Some(l) = n.listener.take() {
            l.shutdown().await;
        }
        let _ = n.raft.shutdown().await;
    }
}

/// A single Raft node (id 1, peers 2 and 3) behind a listener; returns it
/// and a client network for node 2.
async fn single_target(
    server_tls: Option<&ClusterTls>,
    client_tls: Option<&ClusterTls>,
) -> (Node, Network) {
    let tcp = bind().await;
    let addr = tcp.local_addr().expect("addr").to_string();
    let addrs: BTreeMap<NodeId, String> = [
        (1, addr.clone()),
        (2, "127.0.0.1:1".into()),
        (3, "127.0.0.1:1".into()),
    ]
    .into();
    let node = start_node(1, tcp, &addrs, server_tls, raft_config()).await;
    let client = Network::new(net_config(2, [(1, addr)].into(), client_tls));
    (node, client)
}

async fn client_to(net: &Network, target: NodeId, addr: &str) -> PeerClient {
    let mut n = net.clone();
    n.new_client(target, &BasicNode::new(addr)).await
}

fn vote_req(term: u64, from: NodeId) -> VoteRequest<NodeId> {
    VoteRequest::new(Vote::new(term, from), None)
}

fn forward_from(from: NodeId) -> ForwardRequest {
    let c = conn_id(from, 1);
    ForwardRequest {
        from,
        items: vec![(c, 1, EngineInput::Connect(c))],
    }
}

// ------------------------------------------------------------------ tests

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_plaintext_cluster_elects_and_replicates() {
    let nodes = start_cluster(3, None, raft_config()).await;
    replicate_and_check(&nodes, 0, 20).await;
    shutdown(nodes).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_cluster_elects_and_replicates() {
    let pki = Pki::new("cluster CA");
    let nodes = start_cluster(3, Some(&pki), raft_config()).await;
    replicate_and_check(&nodes, 0, 20).await;
    shutdown(nodes).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_rpc_round_trips() {
    let (node, net) = single_target(None, None).await;
    let addr = node.addr.to_string();
    let mut c = client_to(&net, 1, &addr).await;

    // Vote.
    let v = c.vote(vote_req(1, 2), option()).await.expect("vote");
    assert!(v.vote_granted);
    assert_eq!(v.vote, Vote::new(1, 2));

    // AppendEntries from the leader it voted for, with one entry.
    let leader = CommittedLeaderId::new(1, 2);
    let members: BTreeMap<NodeId, BasicNode> =
        [(2, BasicNode::new("x")), (1, BasicNode::new(addr.clone()))].into();
    let membership = Membership::new(vec![members.keys().copied().collect()], members.clone());
    let resp = c
        .append_entries(
            AppendEntriesRequest {
                vote: Vote::new_committed(1, 2),
                prev_log_id: None,
                entries: vec![
                    Entry {
                        log_id: LogId::new(leader, 0),
                        payload: EntryPayload::Membership(membership.clone()),
                    },
                    Entry {
                        log_id: LogId::new(leader, 1),
                        payload: EntryPayload::Normal(req(7)),
                    },
                ],
                leader_commit: Some(LogId::new(leader, 1)),
            },
            option(),
        )
        .await
        .expect("append");
    assert!(matches!(resp, AppendEntriesResponse::Success), "{resp:?}");
    node.raft
        .wait(Some(Duration::from_secs(5)))
        .applied_index_at_least(Some(1), "applied")
        .await
        .expect("applied");
    assert_eq!(node.sm.applied(), vec![req(7)]);

    // InstallSnapshot: a chunk at offset > 0 of an unknown snapshot is a
    // SnapshotMismatch, transported as a value.
    let meta = SnapshotMeta {
        last_log_id: Some(LogId::new(leader, 5)),
        last_membership: StoredMembership::new(Some(LogId::new(leader, 0)), membership),
        snapshot_id: "snap-1".into(),
    };
    let data = postcard::to_stdvec(&vec![req(1), req(2), req(3)]).expect("encode");
    let e = c
        .install_snapshot(
            InstallSnapshotRequest {
                vote: Vote::new_committed(1, 2),
                meta: meta.clone(),
                offset: 3,
                data: data.clone(),
                done: false,
            },
            option(),
        )
        .await
        .expect_err("mismatch");
    match e {
        RPCError::RemoteError(r) => assert!(
            matches!(
                r.source,
                RaftError::APIError(InstallSnapshotError::SnapshotMismatch(_))
            ),
            "{r:?}"
        ),
        other => panic!("unexpected {other:?}"),
    }
    // Then a snapshot in two chunks is installed.
    let (a, b) = data.split_at(data.len() / 2);
    for (offset, chunk, done) in [(0, a, false), (a.len() as u64, b, true)] {
        let r = c
            .install_snapshot(
                InstallSnapshotRequest {
                    vote: Vote::new_committed(1, 2),
                    meta: meta.clone(),
                    offset,
                    data: chunk.to_vec(),
                    done,
                },
                option(),
            )
            .await
            .expect("chunk");
        assert_eq!(r.vote, Vote::new_committed(1, 2));
    }
    node.raft
        .wait(Some(Duration::from_secs(5)))
        .applied_index_at_least(Some(5), "snapshot installed")
        .await
        .expect("installed");
    assert_eq!(node.sm.applied(), vec![req(1), req(2), req(3)]);

    // Forward.
    let r = net.forward(1, forward_from(2)).await.expect("forward");
    assert_eq!(r, ForwardResponse::NotLeader { leader: Some(3) });
    assert_eq!(node.handler.calls(), 1);
    assert_eq!(
        node.handler.last.lock().expect("lock").clone(),
        Some(forward_from(2))
    );
    // Through the transport trait as well.
    let r = ForwardTransport::forward(&net, 1, forward_from(2)).await;
    assert_eq!(r, Ok(ForwardResponse::NotLeader { leader: Some(3) }));

    shutdown(vec![node]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forward_from_another_node_is_rejected() {
    let (node, net) = single_target(None, None).await;
    // `from` is not the connection's node.
    let e = net.forward(1, forward_from(3)).await.expect_err("rejected");
    assert!(matches!(e, ForwardError::Rejected(_)), "{e:?}");
    // A connection owned by another node.
    let mut f = forward_from(2);
    let other = conn_id(3, 9);
    f.items.push((other, 1, EngineInput::Connect(other)));
    let e = net.forward(1, f).await.expect_err("rejected");
    assert!(matches!(e, ForwardError::Rejected(_)), "{e:?}");
    // An input naming another connection.
    let mut f = forward_from(2);
    f.items[0].2 = EngineInput::Disconnect(conn_id(2, 5));
    let e = net.forward(1, f).await.expect_err("rejected");
    assert!(matches!(e, ForwardError::Rejected(_)), "{e:?}");
    assert_eq!(node.handler.calls(), 0);
    // The connection is still usable.
    net.forward(1, forward_from(2)).await.expect("forward");
    assert_eq!(node.handler.calls(), 1);
    shutdown(vec![node]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hello_from_unknown_or_misaddressed_node_is_rejected() {
    let (node, _) = single_target(None, None).await;
    let addr = node.addr.to_string();
    // Node 9 is not a configured peer.
    let net9 = Network::new(net_config(9, [(1, addr.clone())].into(), None));
    let e = net9
        .forward(1, forward_from(9))
        .await
        .expect_err("rejected");
    assert!(
        matches!(e, ForwardError::Unreachable(ref m) if m.contains("rejected: hello rejected")),
        "{e:?}"
    );
    // Node 2 believes node 5 listens there.
    let net2 = Network::new(net_config(2, [(5, addr.clone())].into(), None));
    let e = net2
        .forward(5, forward_from(2))
        .await
        .expect_err("rejected");
    assert!(
        matches!(e, ForwardError::Unreachable(ref m) if m.contains("rejected: hello rejected")),
        "{e:?}"
    );
    // Unknown target address.
    let e = net2.forward(7, forward_from(2)).await.expect_err("unknown");
    assert!(matches!(e, ForwardError::Unreachable(_)), "{e:?}");
    assert_eq!(node.handler.calls(), 0);
    shutdown(vec![node]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unreachable_target_maps_to_unreachable_and_backs_off() {
    // A port with nothing listening.
    let addr = {
        let t = bind().await;
        t.local_addr().expect("addr").to_string()
    };
    let net = Network::new(net_config(2, [(1, addr.clone())].into(), None));
    let mut c = client_to(&net, 1, &addr).await;
    let e = c
        .vote(vote_req(1, 2), option())
        .await
        .expect_err("unreachable");
    assert!(matches!(e, RPCError::Unreachable(_)), "{e:?}");
    // Immediately again: refused by the backoff without dialing.
    let e = c
        .vote(vote_req(1, 2), option())
        .await
        .expect_err("backing off");
    match e {
        RPCError::Unreachable(u) => assert!(u.to_string().contains("backing off"), "{u}"),
        other => panic!("unexpected {other:?}"),
    }
    // The backoff schedule doubles and is capped.
    let mut b = c.backoff();
    let delays: Vec<_> = (0..6).filter_map(|_| b.next()).collect();
    assert_eq!(
        delays,
        [10, 20, 40, 80, 100, 100]
            .map(Duration::from_millis)
            .to_vec()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconnects_after_listener_restart() {
    let tcp = bind().await;
    let addr_s = tcp.local_addr().expect("addr");
    let addrs: BTreeMap<NodeId, String> =
        [(1, addr_s.to_string()), (2, "127.0.0.1:1".into())].into();
    let mut node = start_node(1, tcp, &addrs, None, raft_config()).await;
    let net = Network::new(net_config(2, [(1, addr_s.to_string())].into(), None));
    net.forward(1, forward_from(2)).await.expect("first");

    // Stop the listener: its connections are closed.
    node.listener.take().expect("listener").shutdown().await;
    let e = net.forward(1, forward_from(2)).await.expect_err("down");
    assert!(
        matches!(e, ForwardError::Network(_) | ForwardError::Unreachable(_)),
        "{e:?}"
    );

    // Restart it on the same address; the client re-dials after backoff.
    let tcp = TcpListener::bind(addr_s).await.expect("rebind");
    node.listener = Some(
        ClusterListener::spawn(
            tcp,
            listener_config(1, &[1, 2], None),
            node.raft.clone(),
            node.handler.clone(),
        )
        .expect("listener"),
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match net.forward(1, forward_from(2)).await {
            Ok(_) => break,
            Err(e) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "no reconnect: {e:?}"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
    assert_eq!(node.handler.calls(), 2);
    shutdown(vec![node]).await;
}

/// A fake listener: answers the hello if `answer_hello`, then reads and
/// ignores every request. Counts accepted connections.
async fn silent_server(answer_hello: bool) -> (String, Arc<AtomicUsize>) {
    let tcp = bind().await;
    let addr = tcp.local_addr().expect("addr").to_string();
    let accepts = Arc::new(AtomicUsize::new(0));
    let count = accepts.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = tcp.accept().await else {
                return;
            };
            count.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let Ok(Some(ClientMsg::Hello(_))) =
                    wire::read_frame::<_, ClientMsg>(&mut s, wire::DEFAULT_MAX_FRAME).await
                else {
                    return;
                };
                if answer_hello {
                    let f = wire::encode(
                        &ServerMsg::Hello(ServerHello::Accepted {
                            version: PROTOCOL_VERSION,
                            node_id: 1,
                            max_job_size: bstk_proto::DEFAULT_MAX_JOB_SIZE,
                        }),
                        wire::DEFAULT_MAX_FRAME,
                    )
                    .expect("encode");
                    if wire::write_frame(&mut s, &f).await.is_err() {
                        return;
                    }
                }
                while let Ok(Some(_)) =
                    wire::read_frame::<_, ClientMsg>(&mut s, wire::DEFAULT_MAX_FRAME).await
                {
                }
            });
        }
    });
    (addr, accepts)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_timeouts() {
    let (addr, accepts) = silent_server(true).await;
    let mut cfg = net_config(2, [(1, addr.clone())].into(), None);
    cfg.vote_timeout = Duration::from_millis(100);
    cfg.append_timeout = Duration::from_millis(150);
    cfg.forward_timeout = Duration::from_millis(120);
    let net = Network::new(cfg);
    let mut c = client_to(&net, 1, &addr).await;

    let t0 = tokio::time::Instant::now();
    let e = c.vote(vote_req(1, 2), option()).await.expect_err("timeout");
    let took = t0.elapsed();
    match e {
        RPCError::Timeout(t) => {
            assert_eq!(t.timeout, Duration::from_millis(100));
            assert_eq!((t.id, t.target), (2, 1));
        }
        other => panic!("unexpected {other:?}"),
    }
    assert!(
        took >= Duration::from_millis(100) && took < Duration::from_secs(2),
        "{took:?}"
    );

    // The RPCOption's hard TTL caps the configured timeout.
    let t0 = tokio::time::Instant::now();
    let e = c
        .append_entries(
            AppendEntriesRequest {
                vote: Vote::new_committed(1, 2),
                prev_log_id: None,
                entries: vec![],
                leader_commit: None,
            },
            RPCOption::new(Duration::from_millis(60)),
        )
        .await
        .expect_err("timeout");
    assert!(
        matches!(e, RPCError::Timeout(ref t) if t.timeout == Duration::from_millis(60)),
        "{e:?}"
    );
    assert!(t0.elapsed() < Duration::from_secs(2));

    let e = net.forward(1, forward_from(2)).await.expect_err("timeout");
    assert_eq!(e, ForwardError::Timeout);

    // A timeout fails only its own request: the connection stays up until
    // it has been stalled for the stall bound (1 s here).
    assert_eq!(accepts.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stalled_connection_is_closed_after_the_stall_bound() {
    let (addr, accepts) = silent_server(true).await;
    let mut cfg = net_config(2, [(1, addr.clone())].into(), None);
    cfg.vote_timeout = Duration::from_millis(50);
    cfg.stall_timeout = Duration::from_millis(100);
    let net = Network::new(cfg);
    let mut c = client_to(&net, 1, &addr).await;
    // The bound is max(100 ms, 3 × 50 ms) = 150 ms of no answer at all.
    let t0 = tokio::time::Instant::now();
    let mut timeouts = 0;
    while accepts.load(Ordering::SeqCst) < 2 {
        let e = c.vote(vote_req(1, 2), option()).await.expect_err("timeout");
        assert!(matches!(e, RPCError::Timeout(_)), "{e:?}");
        timeouts += 1;
        assert!(t0.elapsed() < Duration::from_secs(5), "never re-dialed");
    }
    assert!(timeouts >= 4, "re-dialed after {timeouts} timeouts");
    assert!(t0.elapsed() >= Duration::from_millis(150));
}

/// A fake listener that answers every request, in order, `delay` after
/// reading it.
async fn slow_server(delay: Duration) -> (String, Arc<AtomicUsize>) {
    use crate::wire::{RpcRequest, RpcResponse};
    let tcp = bind().await;
    let addr = tcp.local_addr().expect("addr").to_string();
    let accepts = Arc::new(AtomicUsize::new(0));
    let count = accepts.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = tcp.accept().await else {
                return;
            };
            count.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let max = wire::DEFAULT_MAX_FRAME;
                let Ok(Some(ClientMsg::Hello(_))) =
                    wire::read_frame::<_, ClientMsg>(&mut s, max).await
                else {
                    return;
                };
                let hello = ServerMsg::Hello(ServerHello::Accepted {
                    version: PROTOCOL_VERSION,
                    node_id: 1,
                    max_job_size: bstk_proto::DEFAULT_MAX_JOB_SIZE,
                });
                let f = wire::encode(&hello, max).expect("encode");
                if wire::write_frame(&mut s, &f).await.is_err() {
                    return;
                }
                while let Ok(Some(ClientMsg::Request { id, body })) =
                    wire::read_frame::<_, ClientMsg>(&mut s, max).await
                {
                    tokio::time::sleep(delay).await;
                    let body = match body {
                        RpcRequest::AppendEntries(_) => {
                            RpcResponse::AppendEntries(Ok(AppendEntriesResponse::Success))
                        }
                        RpcRequest::Forward(_) => {
                            RpcResponse::Forward(Ok(ForwardResponse::NotLeader { leader: None }))
                        }
                        _ => return,
                    };
                    let f = wire::encode(&ServerMsg::Response { id, body }, max).expect("encode");
                    if wire::write_frame(&mut s, &f).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    (addr, accepts)
}

fn heartbeat() -> AppendEntriesRequest<TypeConfig> {
    AppendEntriesRequest {
        vote: Vote::new_committed(1, 2),
        prev_log_id: None,
        entries: vec![],
        leader_commit: None,
    }
}

/// H2: an AppendEntries that outlives openraft's heartbeat-interval
/// timeout (which drops the call) or our own timeout fails alone; the
/// forward sharing the connection is answered, and nothing re-dials.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_append_does_not_fail_forwards_on_the_same_connection() {
    let (addr, accepts) = slow_server(Duration::from_millis(80)).await;
    let mut cfg = net_config(2, [(1, addr.clone())].into(), None);
    cfg.append_timeout = Duration::from_millis(50);
    cfg.forward_timeout = Duration::from_secs(2);
    let net = Network::new(cfg);
    let mut c = client_to(&net, 1, &addr).await;

    // Our own timeout.
    let e = c
        .append_entries(heartbeat(), option())
        .await
        .expect_err("timeout");
    assert!(matches!(e, RPCError::Timeout(_)), "{e:?}");
    // openraft's outer timeout dropping the call (as replication does
    // with `heartbeat_interval`).
    let mut c2 = client_to(&net, 1, &addr).await;
    let dropped = tokio::time::timeout(
        Duration::from_millis(30),
        c2.append_entries(heartbeat(), option()),
    )
    .await;
    assert!(dropped.is_err());
    assert_eq!(net.pending_len(1), 0, "the dropped call left its slot");

    // A forward behind them is answered on the same connection.
    let r = net.forward(1, forward_from(2)).await.expect("forward");
    assert_eq!(r, ForwardResponse::NotLeader { leader: None });
    assert_eq!(accepts.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connect_timeout_is_unreachable() {
    let (addr, _) = silent_server(false).await;
    let mut cfg = net_config(2, [(1, addr.clone())].into(), None);
    cfg.connect_timeout = Duration::from_millis(100);
    let net = Network::new(cfg);
    let mut c = client_to(&net, 1, &addr).await;
    let e = c
        .vote(vote_req(1, 2), option())
        .await
        .expect_err("no hello answer");
    assert!(matches!(e, RPCError::Unreachable(_)), "{e:?}");
}

#[tokio::test]
async fn oversized_append_asks_openraft_to_split() {
    let mut cfg = net_config(2, [(1, "127.0.0.1:1".into())].into(), None);
    cfg.max_frame = 256;
    let net = Network::new(cfg);
    let mut c = client_to(&net, 1, "127.0.0.1:1").await;
    let leader = CommittedLeaderId::new(1, 2);
    let big = |i| Entry {
        log_id: LogId::new(leader, i),
        payload: EntryPayload::Normal(Request {
            now: i,
            op: Op::Conn {
                seq: 1,
                input: EngineInput::PutStarted {
                    conn: conn_id(2, i),
                    too_big: false,
                },
            },
        }),
    };
    let e = c
        .append_entries(
            AppendEntriesRequest {
                vote: Vote::new_committed(1, 2),
                prev_log_id: None,
                entries: (1..=40).map(big).collect(),
                leader_commit: None,
            },
            option(),
        )
        .await
        .expect_err("too large");
    match e {
        RPCError::PayloadTooLarge(p) => {
            let h = p.entries_hint();
            assert!((1..40).contains(&h), "{h}");
        }
        other => panic!("unexpected {other:?}"),
    }
}

fn put_entry(i: u64, body: usize) -> Entry<TypeConfig> {
    Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 2), i),
        payload: EntryPayload::Normal(Request {
            now: i,
            op: Op::Conn {
                seq: 2,
                input: EngineInput::Command {
                    conn: conn_id(2, 1),
                    cmd: bstk_proto::Command::Put {
                        pri: 0,
                        delay: 0,
                        ttr: 1,
                        body: bytes::Bytes::from(vec![b'x'; body]),
                    },
                },
            },
        }),
    }
}

/// H2 (a): batches are bounded by bytes, not only by the frame size; a
/// single entry larger than the budget is still sent.
#[tokio::test]
async fn append_batches_are_split_by_the_byte_budget() {
    let mut cfg = net_config(2, [(1, "127.0.0.1:1".into())].into(), None);
    cfg.append_budget = 10_000;
    let net = Network::new(cfg);
    let mut c = client_to(&net, 1, "127.0.0.1:1").await;
    let mut req = heartbeat();
    req.entries = (1..=100).map(|i| put_entry(i, 1000)).collect();
    let e = c
        .append_entries(req, option())
        .await
        .expect_err("too large");
    match e {
        RPCError::PayloadTooLarge(p) => {
            let h = p.entries_hint();
            assert!((5..=10).contains(&h), "{h}");
        }
        other => panic!("unexpected {other:?}"),
    }
    // One entry of 50 kB goes (and fails only because nobody listens).
    let mut req = heartbeat();
    req.entries = vec![put_entry(1, 50_000)];
    let e = c
        .append_entries(req, option())
        .await
        .expect_err("no server");
    assert!(matches!(e, RPCError::Unreachable(_)), "{e:?}");
}

async fn raw_hello(addr: SocketAddr, from: NodeId) -> tokio::net::TcpStream {
    let mut s = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let f = wire::encode(
        &ClientMsg::Hello(Hello {
            version: PROTOCOL_VERSION,
            from,
            to: 1,
            max_job_size: bstk_proto::DEFAULT_MAX_JOB_SIZE,
        }),
        wire::DEFAULT_MAX_FRAME,
    )
    .expect("encode");
    wire::write_frame(&mut s, &f).await.expect("hello");
    let a: Option<ServerMsg> = wire::read_frame(&mut s, wire::DEFAULT_MAX_FRAME)
        .await
        .expect("answer");
    assert!(
        matches!(a, Some(ServerMsg::Hello(ServerHello::Accepted { .. }))),
        "{a:?}"
    );
    s
}

/// Reads until EOF (or error); true if the peer closed within 2 s.
async fn closes(s: &mut tokio::net::TcpStream) -> bool {
    let mut buf = [0u8; 256];
    let r = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match s.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
        }
    })
    .await;
    r.is_ok()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn listener_closes_on_bad_frames() {
    let (node, _) = single_target(None, None).await;
    // Oversized frame header after the hello.
    let mut s = raw_hello(node.addr, 2).await;
    s.write_all(&u32::MAX.to_be_bytes()).await.expect("write");
    assert!(closes(&mut s).await);
    // Garbage payload.
    let mut s = raw_hello(node.addr, 2).await;
    s.write_all(&[0, 0, 0, 3, 0xff, 0xff, 0xff])
        .await
        .expect("write");
    assert!(closes(&mut s).await);
    // A request before the hello.
    let mut s = tokio::net::TcpStream::connect(node.addr)
        .await
        .expect("connect");
    let f = wire::encode(
        &ClientMsg::Request {
            id: 1,
            body: wire::RpcRequest::Forward(forward_from(2)),
        },
        wire::DEFAULT_MAX_FRAME,
    )
    .expect("encode");
    s.write_all(&f).await.expect("write");
    assert!(closes(&mut s).await);
    // A second hello.
    let mut s = raw_hello(node.addr, 2).await;
    let f = wire::encode(
        &ClientMsg::Hello(Hello {
            version: PROTOCOL_VERSION,
            from: 2,
            to: 1,
            max_job_size: bstk_proto::DEFAULT_MAX_JOB_SIZE,
        }),
        wire::DEFAULT_MAX_FRAME,
    )
    .expect("encode");
    s.write_all(&f).await.expect("write");
    assert!(closes(&mut s).await);
    assert_eq!(node.handler.calls(), 0);
    shutdown(vec![node]).await;
}

/// Sends a forward on a raw authenticated connection; true if answered.
async fn raw_forward(s: &mut tokio::net::TcpStream, from: NodeId) -> bool {
    let f = wire::encode(
        &ClientMsg::Request {
            id: 1,
            body: wire::RpcRequest::Forward(forward_from(from)),
        },
        wire::DEFAULT_MAX_FRAME,
    )
    .expect("encode");
    if s.write_all(&f).await.is_err() {
        return false;
    }
    let r = tokio::time::timeout(
        Duration::from_secs(2),
        wire::read_frame::<_, ServerMsg>(s, wire::DEFAULT_MAX_FRAME),
    )
    .await;
    matches!(r, Ok(Ok(Some(ServerMsg::Response { id: 1, .. }))))
}

/// A lone node 1 (peers 2 and 3) with listener settings from `edit`.
async fn lone_listener(
    edit: impl FnOnce(&mut ListenerConfig),
) -> (ClusterListener, Raft<TypeConfig>, SocketAddr) {
    let tcp = bind().await;
    let addr = tcp.local_addr().expect("addr");
    let addrs: BTreeMap<NodeId, String> = [(1, addr.to_string()), (2, "127.0.0.1:1".into())].into();
    let net = Network::new(net_config(1, addrs.clone(), None));
    let raft = Raft::new(1, raft_config(), net, MemLog::default(), MemSm::default())
        .await
        .expect("raft");
    let mut cfg = listener_config(1, &[1, 2, 3], None);
    edit(&mut cfg);
    let handler = Arc::new(CountingHandler::default());
    let l = ClusterListener::spawn(tcp, cfg, raft.clone(), handler).expect("listener");
    (l, raft, addr)
}

/// H1: connections that never finish the handshake use their own budget
/// (global and per source address) and time out; they cannot take the
/// slots of authenticated peers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn listener_limits_handshakes_separately_from_peers() {
    let (l, raft, addr) = lone_listener(|c| {
        c.max_handshakes = 1;
        c.max_handshakes_per_ip = 1;
        c.handshake_timeout = Duration::from_millis(300);
    })
    .await;

    // Node 2 is connected and authenticated.
    let mut peer2 = raw_hello(addr, 2).await;
    // A connection that never says hello takes the only handshake slot.
    let mut idle = tokio::net::TcpStream::connect(addr).await.expect("connect");
    tokio::time::sleep(Duration::from_millis(50)).await;
    // Another one is closed at accept.
    let mut second = tokio::net::TcpStream::connect(addr).await.expect("connect");
    assert!(closes(&mut second).await);
    // The authenticated peer is still served.
    assert!(raw_forward(&mut peer2, 2).await);
    // The idle one is closed when the handshake time runs out.
    assert!(closes(&mut idle).await);
    // Then the slot is free again: node 3 gets in, node 2 stays.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut peer3 = raw_hello(addr, 3).await;
    assert!(raw_forward(&mut peer3, 3).await);
    assert!(raw_forward(&mut peer2, 2).await);
    l.shutdown().await;
    let _ = raft.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn listener_limits_handshakes_per_source_address() {
    let (l, raft, addr) = lone_listener(|c| {
        c.max_handshakes = 8;
        c.max_handshakes_per_ip = 2;
        c.handshake_timeout = Duration::from_millis(500);
    })
    .await;
    let _a = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let _b = tokio::net::TcpStream::connect(addr).await.expect("connect");
    tokio::time::sleep(Duration::from_millis(50)).await;
    // The global budget has room, but this address is at its limit.
    let mut c = tokio::net::TcpStream::connect(addr).await.expect("connect");
    assert!(closes(&mut c).await);
    l.shutdown().await;
    let _ = raft.shutdown().await;
}

/// H1: a new authenticated connection of a peer replaces its older one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn newer_peer_connection_replaces_the_older_one() {
    let (l, raft, addr) = lone_listener(|_| {}).await;
    let mut old = raw_hello(addr, 2).await;
    assert!(raw_forward(&mut old, 2).await);
    let mut other = raw_hello(addr, 3).await;
    let mut new = raw_hello(addr, 2).await;
    assert!(closes(&mut old).await);
    assert!(raw_forward(&mut new, 2).await);
    // Node 3's connection is untouched.
    assert!(raw_forward(&mut other, 3).await);
    l.shutdown().await;
    let _ = raft.shutdown().await;
}

#[test]
fn listener_defaults() {
    let c = ListenerConfig::new(1, [1, 2, 3].into(), None);
    assert_eq!(c.handshake_timeout, Duration::from_secs(2));
    assert_eq!(c.max_handshakes_per_ip, 6);
    assert!(c.max_handshakes >= c.max_handshakes_per_ip);
    assert!(c.vote_gate.is_none());
}

// -------------------------------------------------------------------- TLS

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mtls_good_certificates_work() {
    let pki = Pki::new("cluster CA");
    let (node, net) = single_target(Some(&pki.node(1)), Some(&pki.node(2))).await;
    let mut c = client_to(&net, 1, &node.addr.to_string()).await;
    assert!(
        c.vote(vote_req(1, 2), option())
            .await
            .expect("vote")
            .vote_granted
    );
    net.forward(1, forward_from(2)).await.expect("forward");
    assert_eq!(node.handler.calls(), 1);
    shutdown(vec![node]).await;
}

/// Asserts that node 2's `client_tls` cannot reach node 1 and that nothing
/// reached node 1's Raft or forward handler.
async fn assert_rejected(server_tls: ClusterTls, client_tls: ClusterTls, why: &str) {
    let (node, net) = single_target(Some(&server_tls), Some(&client_tls)).await;
    let mut c = client_to(&net, 1, &node.addr.to_string()).await;
    let e = c
        .vote(vote_req(1, 2), option())
        .await
        .expect_err("rejected");
    match e {
        RPCError::Unreachable(u) => assert!(u.to_string().contains(why), "want {why:?}: {u}"),
        other => panic!("unexpected {other:?}"),
    }
    let e = net.forward(1, forward_from(2)).await.expect_err("rejected");
    assert!(matches!(e, ForwardError::Unreachable(_)), "{e:?}");
    assert_eq!(node.handler.calls(), 0);
    assert_eq!(node.raft.metrics().borrow().vote, Vote::default());
    shutdown(vec![node]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mtls_missing_client_certificate_is_rejected() {
    let pki = Pki::new("cluster CA");
    let mut roots = rustls::RootCertStore::empty();
    for c in rustls_pki_types::CertificateDer::pem_slice_iter(pki.ca_pem().as_bytes()) {
        roots.add(c.expect("ca")).expect("add");
    }
    let no_cert = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("versions")
    .with_root_certificates(roots)
    .with_no_client_auth();
    let mut client = pki.node(2);
    client.client = Arc::new(no_cert);
    assert_rejected(pki.node(1), client, "").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mtls_wrong_ca_is_rejected_both_ways() {
    let pki = Pki::new("cluster CA");
    let rogue = Pki::new("rogue CA");
    // The client's certificate is from another CA (it trusts the cluster CA).
    assert_rejected(pki.node(1), tls_with_cert_for(&rogue, &pki, 2), "").await;
    // The server's certificate is from another CA.
    assert_rejected(
        tls_with_cert_for(&rogue, &pki, 1),
        pki.node(2),
        "UnknownIssuer",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mtls_certificate_for_another_node_is_rejected() {
    let pki = Pki::new("cluster CA");
    // Node 2 presents node 3's certificate (a configured peer) and says
    // `from: 2` in the hello.
    assert_rejected(
        pki.node(1),
        tls_with_cert_for(&pki, &pki, 3),
        "rejected: hello rejected",
    )
    .await;
    // The listener at node 1's address presents node 3's certificate: the
    // dialer (expecting bstk-node-1) refuses it.
    assert_rejected(
        tls_with_cert_for(&pki, &pki, 3),
        pki.node(2),
        "certificate not valid for name \"bstk-node-1\"",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mtls_client_to_plaintext_listener_and_back_fail() {
    let pki = Pki::new("cluster CA");
    let (node, _) = single_target(None, None).await;
    let net = Network::new(net_config(
        2,
        [(1, node.addr.to_string())].into(),
        Some(&pki.node(2)),
    ));
    let e = net
        .forward(1, forward_from(2))
        .await
        .expect_err("tls to plaintext");
    assert!(matches!(e, ForwardError::Unreachable(_)), "{e:?}");
    shutdown(vec![node]).await;

    let (node, _) = single_target(Some(&pki.node(1)), None).await;
    let net = Network::new(net_config(2, [(1, node.addr.to_string())].into(), None));
    let e = net
        .forward(1, forward_from(2))
        .await
        .expect_err("plaintext to tls");
    assert!(matches!(e, ForwardError::Unreachable(_)), "{e:?}");
    assert_eq!(node.handler.calls(), 0);
    shutdown(vec![node]).await;
}

#[test]
fn tls_config_rejects_own_certificate_for_another_node() {
    let pki = Pki::new("cluster CA");
    let (cert, key) = pki.leaf(&[node_dns_name(3)]);
    let e = cluster_tls_from_pem(2, cert.as_bytes(), key.as_bytes(), pki.ca_pem().as_bytes())
        .expect_err("wrong name");
    assert!(e.0.contains("bstk-node-2"), "{e}");
    let e =
        cluster_tls_from_pem(2, b"", key.as_bytes(), pki.ca_pem().as_bytes()).expect_err("no cert");
    assert!(e.0.contains("no certificate"), "{e}");
    let (cert, key) = pki.leaf(&[node_dns_name(2)]);
    let e = cluster_tls_from_pem(2, cert.as_bytes(), key.as_bytes(), b"").expect_err("no ca");
    assert!(e.0.contains("cluster.tls.ca"), "{e}");
}

// --------------------------------------------------------------- snapshot

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wiped_node_catches_up_by_chunked_snapshot_over_tls() {
    let pki = Pki::new("cluster CA");
    let mut c = (*raft_config()).clone();
    c.snapshot_max_chunk_size = 64;
    c.max_in_snapshot_log_to_keep = 0;
    c.purge_batch_size = 1;
    let config = Arc::new(c.validate().expect("config"));
    let mut nodes = start_cluster(3, Some(&pki), config.clone()).await;
    replicate_and_check(&nodes, 0, 30).await;

    // Stop the node that is not the leader.
    let leader = wait_leader(&nodes).await;
    let victim_pos = nodes.iter().position(|n| n.id != leader).expect("follower");
    let mut victim = nodes.remove(victim_pos);
    let (vid, vaddr) = (victim.id, victim.addr);
    if let Some(l) = victim.listener.take() {
        l.shutdown().await;
    }
    victim.raft.shutdown().await.expect("shutdown");

    // More writes, then a snapshot and a purge on the leader.
    replicate_and_check(&nodes, 100, 10).await;
    let l = nodes.iter().find(|n| n.id == leader).expect("leader");
    l.raft.trigger().snapshot().await.expect("snapshot");
    let applied = l.raft.metrics().borrow().last_applied.expect("applied");
    l.raft
        .wait(Some(Duration::from_secs(5)))
        .snapshot(applied, "snapshot built")
        .await
        .expect("snapshot");
    l.raft
        .trigger()
        .purge_log(applied.index)
        .await
        .expect("purge");
    l.raft
        .wait(Some(Duration::from_secs(5)))
        .purged(Some(applied), "purged")
        .await
        .expect("purged");

    // Restart the wiped node on its old address with empty storage.
    let addrs: BTreeMap<NodeId, String> = [
        (nodes[0].id, nodes[0].addr.to_string()),
        (nodes[1].id, nodes[1].addr.to_string()),
        (vid, vaddr.to_string()),
    ]
    .into();
    let tcp = TcpListener::bind(vaddr).await.expect("rebind");
    let fresh = start_node(vid, tcp, &addrs, Some(&pki.node(vid)), config).await;
    fresh
        .raft
        .wait(Some(Duration::from_secs(10)))
        .applied_index_at_least(Some(applied.index), "caught up")
        .await
        .expect("caught up");
    let snap = fresh.raft.metrics().borrow().snapshot;
    assert!(snap.is_some_and(|s| s.index >= applied.index), "{snap:?}");
    nodes.push(fresh);
    replicate_and_check(&nodes, 200, 5).await;
    shutdown(nodes).await;
}

// ------------------------------------------------ protocol version 2 (P3-T4)

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_requests_are_checked_and_served() {
    let (node, net) = single_target(None, None).await;
    // Drain mode and dropping the sender itself are allowed.
    for op in [
        Op::SetDraining(true),
        Op::DropNode {
            node: 2,
            up_to_local: 5,
        },
    ] {
        let r = net
            .control(
                1,
                ControlRequest {
                    from: 2,
                    op: op.clone(),
                },
            )
            .await
            .expect("control");
        assert_eq!(r, ControlResponse::Accepted { index: Some(7) });
    }
    // Everything else is refused before the handler sees it.
    for (from, op) in [
        (3, Op::SetDraining(true)),
        (
            2,
            Op::DropNode {
                node: 3,
                up_to_local: 5,
            },
        ),
        (2, Op::Tick),
        (
            2,
            Op::Conn {
                seq: 1,
                input: EngineInput::Connect(conn_id(2, 1)),
            },
        ),
    ] {
        let e = net
            .control(1, ControlRequest { from, op })
            .await
            .expect_err("rejected");
        assert!(matches!(e, ForwardError::Rejected(_)), "{e:?}");
    }
    let seen = node.handler.controls.lock().expect("lock").clone();
    assert_eq!(
        seen,
        vec![
            ControlRequest {
                from: 2,
                op: Op::SetDraining(true)
            },
            ControlRequest {
                from: 2,
                op: Op::DropNode {
                    node: 2,
                    up_to_local: 5,
                }
            },
        ]
    );
    shutdown(vec![node]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_control_handler_refuses() {
    struct Plain;
    impl ForwardHandler for Plain {
        async fn forward(&self, _req: ForwardRequest) -> ForwardResponse {
            ForwardResponse::Accepted
        }
    }
    let tcp = bind().await;
    let addr = tcp.local_addr().expect("addr").to_string();
    let addrs: BTreeMap<NodeId, String> = [(1, addr.clone()), (2, "127.0.0.1:1".into())].into();
    let net1 = Network::new(net_config(1, addrs, None));
    let raft = Raft::new(1, raft_config(), net1, MemLog::default(), MemSm::default())
        .await
        .expect("raft");
    let l = ClusterListener::spawn(
        tcp,
        listener_config(1, &[1, 2], None),
        raft.clone(),
        Arc::new(Plain),
    )
    .expect("listener");
    let net = Network::new(net_config(2, [(1, addr)].into(), None));
    let r = net
        .control(
            1,
            ControlRequest {
                from: 2,
                op: Op::SetDraining(true),
            },
        )
        .await
        .expect("control");
    assert_eq!(r, ControlResponse::NotLeader { leader: None });
    l.shutdown().await;
    let _ = raft.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_job_size_mismatch_is_rejected() {
    let (node, _) = single_target(None, None).await;
    let addr = node.addr.to_string();
    let mut cfg = net_config(2, [(1, addr.clone())].into(), None);
    cfg.max_job_size = bstk_proto::DEFAULT_MAX_JOB_SIZE + 1;
    let net = Network::new(cfg);
    let e = net.forward(1, forward_from(2)).await.expect_err("rejected");
    assert!(
        matches!(e, ForwardError::Unreachable(ref m) if m.contains("max_job_size mismatch")),
        "{e:?}"
    );
    assert_eq!(node.handler.calls(), 0);

    // The dialer checks the listener's value too (a listener that accepts
    // anything but reports another size).
    let tcp = bind().await;
    let fake = tcp.local_addr().expect("addr");
    tokio::spawn(async move {
        let Ok((mut s, _)) = tcp.accept().await else {
            return;
        };
        let _ = wire::read_frame::<_, ClientMsg>(&mut s, wire::DEFAULT_MAX_FRAME).await;
        let f = wire::encode(
            &ServerMsg::Hello(ServerHello::Accepted {
                version: PROTOCOL_VERSION,
                node_id: 1,
                max_job_size: 5,
            }),
            wire::DEFAULT_MAX_FRAME,
        )
        .expect("encode");
        let _ = wire::write_frame(&mut s, &f).await;
        let _ = wire::read_frame::<_, ClientMsg>(&mut s, wire::DEFAULT_MAX_FRAME).await;
    });
    let net = Network::new(net_config(2, [(1, fake.to_string())].into(), None));
    let e = net.forward(1, forward_from(2)).await.expect_err("rejected");
    assert!(
        matches!(e, ForwardError::Unreachable(ref m) if m.contains("max_job_size mismatch")),
        "{e:?}"
    );
    shutdown(vec![node]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn old_protocol_version_is_rejected() {
    let (node, _) = single_target(None, None).await;
    let mut s = tokio::net::TcpStream::connect(node.addr)
        .await
        .expect("connect");
    let f = wire::encode(
        &ClientMsg::Hello(Hello {
            version: 1,
            from: 2,
            to: 1,
            max_job_size: bstk_proto::DEFAULT_MAX_JOB_SIZE,
        }),
        wire::DEFAULT_MAX_FRAME,
    )
    .expect("encode");
    wire::write_frame(&mut s, &f).await.expect("hello");
    let a: Option<ServerMsg> = wire::read_frame(&mut s, wire::DEFAULT_MAX_FRAME)
        .await
        .expect("answer");
    assert!(
        matches!(a, Some(ServerMsg::Hello(ServerHello::Rejected { ref reason })) if reason == crate::listener::REJECT_VERSION),
        "{a:?}"
    );
    shutdown(vec![node]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn last_response_tracks_answers() {
    let (node, net) = single_target(None, None).await;
    assert_eq!(net.last_response(1), None);
    let before = std::time::Instant::now();
    let _ = net.forward(1, forward_from(2)).await.expect("forward");
    let at = net.last_response(1).expect("a response arrived");
    assert!(at >= before);
    assert_eq!(net.last_response(3), None);
    shutdown(vec![node]).await;
}

// ------------------------------------------------------------- P3-FA fixes

/// Like [`single_target`], with `edit` applied to the listener config.
async fn single_target_with(edit: impl FnOnce(&mut ListenerConfig)) -> (Node, Network) {
    let tcp = bind().await;
    let addr = tcp.local_addr().expect("addr");
    let addrs: BTreeMap<NodeId, String> = [
        (1, addr.to_string()),
        (2, "127.0.0.1:1".into()),
        (3, "127.0.0.1:1".into()),
    ]
    .into();
    let net = Network::new(net_config(1, addrs.clone(), None));
    let sm = MemSm::default();
    let raft = Raft::new(1, raft_config(), net, MemLog::default(), sm.clone())
        .await
        .expect("raft");
    let handler = Arc::new(CountingHandler::default());
    let mut cfg = listener_config(1, &[1, 2, 3], None);
    edit(&mut cfg);
    let listener =
        ClusterListener::spawn(tcp, cfg, raft.clone(), handler.clone()).expect("listener");
    let client = Network::new(net_config(2, [(1, addr.to_string())].into(), None));
    let node = Node {
        id: 1,
        raft,
        sm,
        handler,
        listener: Some(listener),
        addr,
    };
    (node, client)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closed_vote_gate_refuses_votes_without_touching_raft() {
    let gate = Arc::new(VoteGate::new(false));
    let g = gate.clone();
    let (node, net) = single_target_with(move |c| c.vote_gate = Some(g)).await;
    let mut c = client_to(&net, 1, &node.addr.to_string()).await;

    let e = c.vote(vote_req(5, 2), option()).await.expect_err("refused");
    assert!(matches!(e, RPCError::Network(_)), "{e:?}");
    assert_eq!(gate.refused(), 1);
    // Raft never saw the candidate's term.
    assert_eq!(node.raft.metrics().borrow().current_term, 0);
    // Other requests on the same connection are served.
    let r = net.forward(1, forward_from(2)).await.expect("forward");
    assert_eq!(r, ForwardResponse::NotLeader { leader: Some(3) });

    gate.open();
    let v = c.vote(vote_req(5, 2), option()).await.expect("vote");
    assert!(v.vote_granted);
    assert_eq!(gate.refused(), 1);
    gate.close();
    assert!(c.vote(vote_req(6, 3), option()).await.is_err());
    assert_eq!(gate.refused(), 2);
    shutdown(vec![node]).await;
}

/// H3: chunk offsets over the wire. A retransmitted chunk and a restart
/// from offset 0 (same snapshot id) are accepted; a chunk that would
/// leave a gap is refused (as a remote storage error) without disturbing
/// the node, and the snapshot then completes normally.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_chunks_cannot_leave_gaps() {
    let (node, net) = single_target(None, None).await;
    let addr = node.addr.to_string();
    let mut c = client_to(&net, 1, &addr).await;
    let leader = CommittedLeaderId::new(1, 2);
    let members: BTreeMap<NodeId, BasicNode> =
        [(2, BasicNode::new("x")), (1, BasicNode::new(addr.clone()))].into();
    let membership = Membership::new(vec![members.keys().copied().collect()], members);
    let meta = SnapshotMeta {
        last_log_id: Some(LogId::new(leader, 5)),
        last_membership: StoredMembership::new(Some(LogId::new(leader, 0)), membership),
        snapshot_id: "snap-gap".into(),
    };
    let data = postcard::to_stdvec(&vec![req(1), req(2), req(3)]).expect("encode");
    let (a, b) = data.split_at(data.len() / 2);
    let chunk = |offset: u64, bytes: &[u8], done: bool| InstallSnapshotRequest {
        vote: Vote::new_committed(1, 2),
        meta: meta.clone(),
        offset,
        data: bytes.to_vec(),
        done,
    };
    let alen = a.len() as u64;
    c.install_snapshot(chunk(0, a, false), option())
        .await
        .expect("first chunk");
    // Retransmit of the first chunk.
    c.install_snapshot(chunk(0, a, false), option())
        .await
        .expect("retransmit");
    // A chunk far beyond the received bytes.
    let e = c
        .install_snapshot(chunk(1 << 40, b, false), option())
        .await
        .expect_err("gap");
    assert!(
        matches!(e, RPCError::RemoteError(ref r) if matches!(r.source, RaftError::Fatal(_))),
        "{e:?}"
    );
    let e = c
        .install_snapshot(chunk(alen + 1, b, false), option())
        .await
        .expect_err("gap of one byte");
    assert!(matches!(e, RPCError::RemoteError(_)), "{e:?}");
    // The node is fine; the sender restarts from 0 with the same id.
    c.install_snapshot(chunk(0, a, false), option())
        .await
        .expect("restart");
    c.install_snapshot(chunk(alen, b, true), option())
        .await
        .expect("last chunk");
    node.raft
        .wait(Some(Duration::from_secs(5)))
        .applied_index_at_least(Some(5), "snapshot installed")
        .await
        .expect("installed");
    assert_eq!(node.sm.applied(), vec![req(1), req(2), req(3)]);
    shutdown(vec![node]).await;
}
