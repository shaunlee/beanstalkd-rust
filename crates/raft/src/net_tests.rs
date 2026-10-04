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
use crate::listener::{ClusterListener, ListenerConfig, REJECT_HELLO, REJECT_NOT_MEMBER, VoteGate};
use crate::test_store::{MemLog, MemSm};
use crate::tls::{ClusterTls, cluster_tls_from_pem, node_dns_name};
use crate::wire::{self, ClientMsg, Hello, PROTOCOL_VERSION, ServerHello, ServerMsg};
use crate::{ForwardRequest, ForwardResponse, NodeId, Op, Request, TypeConfig, conn_id};

#[derive(Default)]
struct CountingHandler {
    calls: AtomicUsize,
    last: Mutex<Option<ForwardRequest>>,
    controls: Mutex<Vec<ControlRequest>>,
    admins: Mutex<Vec<(wire::AdminRequest, SocketAddr)>>,
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

    async fn admin(&self, req: wire::AdminRequest, from: SocketAddr) -> wire::AdminResponse {
        self.admins.lock().expect("lock").push((req, from));
        wire::AdminResponse::Conflict { current: None }
    }
}

/// Implements only `forward`: the trait's defaults answer the rest.
struct PlainHandler;

impl ForwardHandler for PlainHandler {
    async fn forward(&self, _req: ForwardRequest) -> ForwardResponse {
        ForwardResponse::Accepted
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

    fn leaf(&self, names: &[String]) -> (String, String) {
        self.leaf_with(
            names,
            vec![
                ExtendedKeyUsagePurpose::ServerAuth,
                ExtendedKeyUsagePurpose::ClientAuth,
            ],
        )
    }

    fn leaf_with(&self, names: &[String], ekus: Vec<ExtendedKeyUsagePurpose>) -> (String, String) {
        let mut params = CertificateParams::new(names.to_vec()).expect("params");
        params
            .distinguished_name
            .push(DnType::CommonName, "bstk test node");
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
        ];
        params.extended_key_usages = ekus;
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

    let v = c.vote(vote_req(1, 2), option()).await.expect("vote");
    assert!(v.vote_granted);
    assert_eq!(v.vote, Vote::new(1, 2));

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

    let r = net.forward(1, forward_from(2)).await.expect("forward");
    assert_eq!(r, ForwardResponse::NotLeader { leader: Some(3) });
    assert_eq!(node.handler.calls(), 1);
    assert_eq!(
        node.handler.last.lock().expect("lock").clone(),
        Some(forward_from(2))
    );
    let r = ForwardTransport::forward(&net, 1, forward_from(2)).await;
    assert_eq!(r, Ok(ForwardResponse::NotLeader { leader: Some(3) }));

    shutdown(vec![node]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forward_from_another_node_is_rejected() {
    let (node, net) = single_target(None, None).await;
    let e = net.forward(1, forward_from(3)).await.expect_err("rejected");
    assert!(matches!(e, ForwardError::Rejected(_)), "{e:?}");
    let mut f = forward_from(2);
    let other = conn_id(3, 9);
    f.items.push((other, 1, EngineInput::Connect(other)));
    let e = net.forward(1, f).await.expect_err("rejected");
    assert!(matches!(e, ForwardError::Rejected(_)), "{e:?}");
    let mut f = forward_from(2);
    f.items[0].2 = EngineInput::Disconnect(conn_id(2, 5));
    let e = net.forward(1, f).await.expect_err("rejected");
    assert!(matches!(e, ForwardError::Rejected(_)), "{e:?}");
    assert_eq!(node.handler.calls(), 0);
    net.forward(1, forward_from(2)).await.expect("forward");
    assert_eq!(node.handler.calls(), 1);
    shutdown(vec![node]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hello_from_unknown_or_misaddressed_node_is_rejected() {
    let (node, _) = single_target(None, None).await;
    let addr = node.addr.to_string();
    // (Plaintext proves no identity, so a stranger learns why.)
    let net9 = Network::new(net_config(9, [(1, addr.clone())].into(), None));
    let e = net9
        .forward(1, forward_from(9))
        .await
        .expect_err("rejected");
    assert!(
        matches!(e, ForwardError::Unreachable(ref m)
            if m.contains(&format!("rejected: {REJECT_NOT_MEMBER}"))),
        "{e:?}"
    );
    let net2 = Network::new(net_config(2, [(5, addr.clone())].into(), None));
    let e = net2
        .forward(5, forward_from(2))
        .await
        .expect_err("rejected");
    assert!(
        matches!(e, ForwardError::Unreachable(ref m) if m.contains("rejected: hello rejected")),
        "{e:?}"
    );
    let e = net2.forward(7, forward_from(2)).await.expect_err("unknown");
    assert!(matches!(e, ForwardError::Unreachable(_)), "{e:?}");
    assert_eq!(node.handler.calls(), 0);
    shutdown(vec![node]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unreachable_target_maps_to_unreachable_and_backs_off() {
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
    let e = c
        .vote(vote_req(1, 2), option())
        .await
        .expect_err("backing off");
    match e {
        RPCError::Unreachable(u) => assert!(u.to_string().contains("backing off"), "{u}"),
        other => panic!("unexpected {other:?}"),
    }
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

    node.listener.take().expect("listener").shutdown().await;
    let e = net.forward(1, forward_from(2)).await.expect_err("down");
    assert!(
        matches!(e, ForwardError::Network(_) | ForwardError::Unreachable(_)),
        "{e:?}"
    );

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
    let mut s = raw_hello(node.addr, 2).await;
    s.write_all(&u32::MAX.to_be_bytes()).await.expect("write");
    assert!(closes(&mut s).await);
    let mut s = raw_hello(node.addr, 2).await;
    s.write_all(&[0, 0, 0, 3, 0xff, 0xff, 0xff])
        .await
        .expect("write");
    assert!(closes(&mut s).await);
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

    let mut peer2 = raw_hello(addr, 2).await;
    let mut idle = tokio::net::TcpStream::connect(addr).await.expect("connect");
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut second = tokio::net::TcpStream::connect(addr).await.expect("connect");
    assert!(closes(&mut second).await);
    assert!(raw_forward(&mut peer2, 2).await);
    assert!(closes(&mut idle).await);
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
    assert_eq!(c.max_admin_conns, 4);
    assert_eq!(c.admin_idle_timeout, Duration::from_secs(60));
}

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
    let e = net.status(1).await.expect_err("rejected");
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

    let leader = wait_leader(&nodes).await;
    let victim_pos = nodes.iter().position(|n| n.id != leader).expect("follower");
    let mut victim = nodes.remove(victim_pos);
    let (vid, vaddr) = (victim.id, victim.addr);
    if let Some(l) = victim.listener.take() {
        l.shutdown().await;
    }
    victim.raft.shutdown().await.expect("shutdown");

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_requests_are_checked_and_served() {
    let (node, net) = single_target(None, None).await;
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
    use PlainHandler as Plain;
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

/// Older peers (version 3 is 0.5.x) and admin tools of another version
/// are refused with the version reason: there is no negotiation (P6-T2).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn old_protocol_version_is_rejected() {
    assert_eq!(PROTOCOL_VERSION, 4);
    let (node, _) = single_target(None, None).await;
    let mut hellos: Vec<ClientMsg> = [1, 3, PROTOCOL_VERSION + 1]
        .into_iter()
        .map(|version| {
            ClientMsg::Hello(Hello {
                version,
                from: 2,
                to: 1,
                max_job_size: bstk_proto::DEFAULT_MAX_JOB_SIZE,
            })
        })
        .collect();
    hellos.push(ClientMsg::AdminHello(wire::AdminHello {
        version: 3,
        to: None,
    }));
    for h in hellos {
        let mut s = tokio::net::TcpStream::connect(node.addr)
            .await
            .expect("connect");
        let f = wire::encode(&h, wire::DEFAULT_MAX_FRAME).expect("encode");
        wire::write_frame(&mut s, &f).await.expect("hello");
        let a: Option<ServerMsg> = wire::read_frame(&mut s, wire::DEFAULT_MAX_FRAME)
            .await
            .expect("answer");
        assert!(
            matches!(a, Some(ServerMsg::Hello(ServerHello::Rejected { ref reason })) if reason == crate::listener::REJECT_VERSION),
            "{h:?}: {a:?}"
        );
        assert!(closes(&mut s).await);
    }
    assert_eq!(node.handler.calls(), 0);
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
    assert_eq!(node.raft.metrics().borrow().current_term, 0);
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
    c.install_snapshot(chunk(0, a, false), option())
        .await
        .expect("retransmit");
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

struct FixedStatus(crate::status::NodeStatus);

impl crate::status::StatusSource for FixedStatus {
    fn status(&self) -> crate::status::NodeStatus {
        self.0
    }
}

/// P3-FC: a listener started before Raft answers status probes from its
/// status source, refuses everything else until the service is installed
/// (votes through the gate first, so they are counted), and serves
/// normally afterwards. Probes need an accepted hello like any request.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_probes_are_answered_before_raft_runs() {
    use crate::status::NodeStatus;
    let tcp = bind().await;
    let addr = tcp.local_addr().expect("addr").to_string();
    let status = NodeStatus {
        vote: Some(Vote::new_committed(4, 3)),
        last_log_id: Some(LogId::new(CommittedLeaderId::new(4, 3), 17)),
        committed: Some(LogId::new(CommittedLeaderId::new(4, 3), 15)),
        has_state: true,
    };
    let gate = Arc::new(VoteGate::new(false));
    let mut cfg = listener_config(1, &[1, 2, 3], None);
    cfg.status = Some(Arc::new(FixedStatus(status)));
    cfg.vote_gate = Some(gate.clone());
    let (listener, slot) =
        ClusterListener::spawn_deferred::<CountingHandler>(tcp, cfg).expect("listener");
    let net = Network::new(net_config(2, [(1, addr.clone())].into(), None));

    assert_eq!(net.status(1).await.expect("status"), status);
    let e = net
        .forward(1, forward_from(2))
        .await
        .expect_err("not started");
    assert!(
        matches!(&e, ForwardError::Rejected(m) if m.contains("not running")),
        "{e:?}"
    );
    let mut c = client_to(&net, 1, &addr).await;
    assert!(c.vote(vote_req(1, 2), option()).await.is_err());
    assert_eq!(gate.refused(), 1);
    gate.open();
    assert!(c.vote(vote_req(1, 2), option()).await.is_err());
    assert_eq!(gate.refused(), 1);
    assert!(c.append_entries(heartbeat(), option()).await.is_err());

    let stranger = Network::new(net_config(9, [(1, addr.clone())].into(), None));
    let e = stranger.status(1).await.expect_err("unknown peer");
    assert!(matches!(e, ForwardError::Unreachable(_)), "{e:?}");

    let raft_net = Network::new(net_config(1, [(1, addr.clone())].into(), None));
    let raft = Raft::new(
        1,
        raft_config(),
        raft_net,
        MemLog::default(),
        MemSm::default(),
    )
    .await
    .expect("raft");
    let handler = Arc::new(CountingHandler::default());
    assert!(slot.set(raft.clone(), handler.clone()));
    assert!(!slot.set(raft.clone(), handler.clone()));
    assert_eq!(
        net.forward(1, forward_from(2)).await.expect("forward"),
        ForwardResponse::NotLeader { leader: Some(3) }
    );
    assert_eq!(handler.calls(), 1);
    assert_eq!(net.status(1).await.expect("status"), status);
    listener.shutdown().await;
    let _ = raft.shutdown().await;

    let (l, raft, addr) = lone_listener(|_| {}).await;
    let net = Network::new(net_config(2, [(1, addr.to_string())].into(), None));
    let e = net.status(1).await.expect_err("no source");
    assert!(
        matches!(&e, ForwardError::Rejected(m) if m.contains("not served")),
        "{e:?}"
    );
    l.shutdown().await;
    let _ = raft.shutdown().await;
}

/// Sends a hello from `from` to node 1 and returns the listener's answer.
async fn hello_answer(addr: SocketAddr, from: NodeId) -> Option<ServerMsg> {
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
    wire::read_frame(&mut s, wire::DEFAULT_MAX_FRAME)
        .await
        .expect("answer")
}

fn rejected_with(a: &Option<ServerMsg>, reason: &str) -> bool {
    matches!(a, Some(ServerMsg::Hello(ServerHello::Rejected { reason: r })) if r == reason)
}

/// P6-T1: replacing the allowlist closes the live connections of nodes
/// that left, answers their hellos with the specific reason, admits new
/// members, and sizes the per-address handshake budget by the membership.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn allowlist_update_closes_departed_peers_and_admits_new_ones() {
    let (l, raft, addr) = lone_listener(|_| {}).await;
    let allow = l.allowlist();
    assert_eq!(allow.get(), [1, 2, 3].into());
    assert_eq!(allow.handshakes_per_ip(), 6);
    let mut peer2 = raw_hello(addr, 2).await;
    let mut peer3 = raw_hello(addr, 3).await;
    assert!(rejected_with(
        &hello_answer(addr, 4).await,
        REJECT_NOT_MEMBER
    ));

    // 3 removed, 4 and 5 added (a joint configuration names them all).
    allow.set([1, 2, 4, 5, 6].into());
    assert!(
        closes(&mut peer3).await,
        "a removed peer kept its connection"
    );
    assert!(raw_forward(&mut peer2, 2).await);
    assert!(rejected_with(
        &hello_answer(addr, 3).await,
        REJECT_NOT_MEMBER
    ));
    let mut peer4 = raw_hello(addr, 4).await;
    assert!(raw_forward(&mut peer4, 4).await);
    assert_eq!(allow.handshakes_per_ip(), 10);
    // Shrinking never goes below the configured budget.
    allow.set([1, 2].into());
    assert_eq!(allow.handshakes_per_ip(), 6);
    assert!(closes(&mut peer4).await);
    l.shutdown().await;
    let _ = raft.shutdown().await;
}

/// P6-T1: identity is checked before membership, so only a node that proves
/// its id is told that it is not a member; anyone else gets the generic
/// reason.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mtls_identity_is_checked_before_membership() {
    let pki = Pki::new("cluster CA");
    let (node, _) = single_target(Some(&pki.node(1)), None).await;
    let addr = node.addr.to_string();
    // Node 9 with its own certificate: authenticated, not a member.
    let tls9 = pki.node(9);
    let net9 = Network::new(net_config(9, [(1, addr.clone())].into(), Some(&tls9)));
    let e = net9.status(1).await.expect_err("rejected");
    assert!(
        matches!(e, ForwardError::Unreachable(ref m) if m.contains(REJECT_NOT_MEMBER)),
        "{e:?}"
    );
    // Node 2's certificate claiming id 9: the identity check fails first.
    let tls2 = pki.node(2);
    let net9 = Network::new(net_config(9, [(1, addr.clone())].into(), Some(&tls2)));
    let e = net9.status(1).await.expect_err("rejected");
    assert!(
        matches!(e, ForwardError::Unreachable(ref m)
            if m.contains(&format!("rejected: {REJECT_HELLO}"))),
        "{e:?}"
    );
    assert_eq!(node.handler.calls(), 0);
    shutdown(vec![node]).await;
}

/// P6-T1 address book: a target without a config address is dialed at its
/// membership address; a config address overrides the membership's; a
/// changed address replaces the connection slot without losing the
/// target's last response time.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn address_book_prefers_overrides_then_membership() {
    let (node, _) = single_target(None, None).await;
    let addr = node.addr.to_string();
    let dead = "127.0.0.1:1".to_string();

    let net = Network::new(net_config(2, BTreeMap::new(), None));
    assert_eq!(net.address(1), None);
    let e = net
        .forward(1, forward_from(2))
        .await
        .expect_err("no address");
    assert!(matches!(e, ForwardError::Unreachable(_)), "{e:?}");
    net.set_members([(1, addr.clone()), (2, dead.clone())].into());
    assert_eq!(net.address(1).as_deref(), Some(addr.as_str()));
    net.forward(1, forward_from(2))
        .await
        .expect("membership address");
    let heard = net.last_response(1).expect("answered");

    // The membership moves node 1 to a dead address: the slot follows it.
    net.set_members([(1, dead.clone())].into());
    let e = net.forward(1, forward_from(2)).await.expect_err("moved");
    assert!(
        matches!(e, ForwardError::Unreachable(ref m) if m.contains(&dead)),
        "{e:?}"
    );
    assert_eq!(net.last_response(1), Some(heard), "liveness lost on a move");
    net.set_members([(1, addr.clone())].into());
    net.forward(1, forward_from(2)).await.expect("moved back");

    // A config override wins over the membership address.
    let over = Network::new(net_config(2, [(1, addr.clone())].into(), None));
    over.set_members([(1, dead.clone())].into());
    assert_eq!(over.address(1).as_deref(), Some(addr.as_str()));
    over.forward(1, forward_from(2)).await.expect("override");

    // Raft RPCs use openraft's address only when the book has none, and
    // resolve on every call (a client created before a move follows it).
    let raft_net = Network::new(net_config(2, BTreeMap::new(), None));
    let mut c = client_to(&raft_net, 1, &addr).await;
    c.vote(vote_req(1, 2), option())
        .await
        .expect("BasicNode address");
    raft_net.set_members([(1, dead.clone())].into());
    let e = c.vote(vote_req(2, 2), option()).await.expect_err("moved");
    assert!(matches!(e, RPCError::Unreachable(_)), "{e:?}");
    shutdown(vec![node]).await;
}

// ---- P6-T2: StatusEx and the admin channel ----

struct FixedStatusEx(crate::status::NodeStatusEx);

impl crate::status::StatusSource for FixedStatusEx {
    fn status(&self) -> crate::status::NodeStatus {
        self.0.status
    }

    fn status_ex(&self) -> crate::status::NodeStatusEx {
        self.0.clone()
    }
}

fn sample_status_ex() -> crate::status::NodeStatusEx {
    let lid = |i| LogId::new(CommittedLeaderId::new(4, 3), i);
    crate::status::NodeStatusEx {
        status: crate::status::NodeStatus {
            vote: Some(Vote::new_committed(4, 3)),
            last_log_id: Some(lid(17)),
            committed: Some(lid(15)),
            has_state: true,
        },
        raft_running: false,
        rejoining: true,
        term: 4,
        leader: None,
        last_applied: Some(lid(12)),
        highest_member: 4,
        membership: crate::status::MembershipView {
            log_id: Some(lid(10)),
            committed: true,
            configs: vec![[1, 2, 3].into()],
            nodes: (1..=4).map(|i| (i, format!("127.0.0.1:{i}"))).collect(),
        },
    }
}

/// A listener for node 1 (peers 2 and 3) answering status from
/// [`sample_status_ex`]; with `raft`, Raft (a single-node `MemLog`) and a
/// counting handler are installed.
async fn admin_target(
    tls: Option<&ClusterTls>,
    raft: bool,
    edit: impl FnOnce(&mut ListenerConfig),
) -> (
    ClusterListener,
    Option<Raft<TypeConfig>>,
    Arc<CountingHandler>,
    SocketAddr,
) {
    let tcp = bind().await;
    let addr = tcp.local_addr().expect("addr");
    let mut cfg = listener_config(1, &[1, 2, 3], tls);
    cfg.status = Some(Arc::new(FixedStatusEx(sample_status_ex())));
    edit(&mut cfg);
    let (l, slot) = ClusterListener::spawn_deferred::<CountingHandler>(tcp, cfg).expect("listener");
    let handler = Arc::new(CountingHandler::default());
    let raft = if raft {
        let net = Network::new(net_config(1, [(1, addr.to_string())].into(), None));
        let r = Raft::new(1, raft_config(), net, MemLog::default(), MemSm::default())
            .await
            .expect("raft");
        assert!(slot.set(r.clone(), handler.clone()));
        Some(r)
    } else {
        None
    };
    (l, raft, handler, addr)
}

trait TestIo: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> TestIo for T {}

impl Pki {
    /// A client-only certificate (EKU clientAuth), as an operator's.
    fn client_leaf(&self, names: &[String]) -> (String, String) {
        self.leaf_with(names, vec![ExtendedKeyUsagePurpose::ClientAuth])
    }

    /// A client config presenting `cert` and trusting `trust`'s CA.
    fn client_config(trust: &Pki, (cert, key): (String, String)) -> Arc<rustls::ClientConfig> {
        let mut roots = rustls::RootCertStore::empty();
        for c in rustls_pki_types::CertificateDer::pem_slice_iter(trust.ca_pem().as_bytes()) {
            roots.add(c.expect("ca")).expect("add");
        }
        let certs = rustls_pki_types::CertificateDer::pem_slice_iter(cert.as_bytes())
            .collect::<Result<Vec<_>, _>>()
            .expect("certs");
        let key = rustls_pki_types::PrivateKeyDer::from_pem_slice(key.as_bytes()).expect("key");
        let c = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("versions")
        .with_root_certificates(roots)
        .with_client_auth_cert(certs, key)
        .expect("client auth");
        Arc::new(c)
    }

    /// The operator's TLS client config: SAN `bstk-admin`, clientAuth only.
    fn admin(&self) -> Arc<rustls::ClientConfig> {
        Pki::client_config(
            self,
            self.client_leaf(&[crate::tls::ADMIN_DNS_NAME.to_string()]),
        )
    }
}

/// Connects to node 1 at `addr` (TLS when `tls` is given), sends `hello`
/// and returns the stream and the listener's answer, or why there was
/// none (a TLS failure, a closed connection).
async fn hello_with(
    addr: SocketAddr,
    tls: Option<Arc<rustls::ClientConfig>>,
    hello: &ClientMsg,
) -> Result<(Box<dyn TestIo>, ServerHello), String> {
    let tcp = tokio::net::TcpStream::connect(addr)
        .await
        .map_err(|e| e.to_string())?;
    let mut io: Box<dyn TestIo> = match tls {
        None => Box::new(tcp),
        Some(c) => Box::new(
            tokio_rustls::TlsConnector::from(c)
                .connect(crate::tls::server_name_for(1).expect("name"), tcp)
                .await
                .map_err(|e| format!("TLS handshake: {e}"))?,
        ),
    };
    let f = wire::encode(hello, wire::DEFAULT_MAX_FRAME).expect("encode");
    wire::write_frame(&mut io, &f)
        .await
        .map_err(|e| e.to_string())?;
    match wire::read_frame::<_, ServerMsg>(&mut io, wire::DEFAULT_MAX_FRAME).await {
        Ok(Some(ServerMsg::Hello(h))) => Ok((io, h)),
        other => Err(format!("{other:?}")),
    }
}

fn admin_hello(to: Option<NodeId>) -> ClientMsg {
    ClientMsg::AdminHello(wire::AdminHello {
        version: PROTOCOL_VERSION,
        to,
    })
}

fn peer_hello(from: NodeId) -> ClientMsg {
    ClientMsg::Hello(Hello {
        version: PROTOCOL_VERSION,
        from,
        to: 1,
        max_job_size: bstk_proto::DEFAULT_MAX_JOB_SIZE,
    })
}

/// An accepted admin connection.
async fn admin_conn(addr: SocketAddr, tls: Option<Arc<rustls::ClientConfig>>) -> Box<dyn TestIo> {
    let (io, h) = hello_with(addr, tls, &admin_hello(Some(1)))
        .await
        .expect("answer");
    assert!(
        matches!(
            h,
            ServerHello::Accepted {
                node_id: 1,
                version: PROTOCOL_VERSION,
                ..
            }
        ),
        "{h:?}"
    );
    io
}

/// Sends `msg` and returns the next message, `None` if the connection
/// closed (or failed) instead.
async fn exchange(io: &mut Box<dyn TestIo>, msg: &ClientMsg) -> Option<ServerMsg> {
    let f = wire::encode(msg, wire::DEFAULT_MAX_FRAME).expect("encode");
    wire::write_frame(io, &f).await.ok()?;
    tokio::time::timeout(
        Duration::from_secs(2),
        wire::read_frame::<_, ServerMsg>(io, wire::DEFAULT_MAX_FRAME),
    )
    .await
    .expect("answer or close in time")
    .ok()?
}

async fn admin_call(
    io: &mut Box<dyn TestIo>,
    id: u64,
    body: wire::AdminRequest,
) -> wire::AdminResponse {
    match exchange(io, &ClientMsg::Admin { id, body }).await {
        Some(ServerMsg::Admin { id: got, body }) if got == id => body,
        other => panic!("admin answer expected: {other:?}"),
    }
}

fn mutating_requests() -> Vec<wire::AdminRequest> {
    let expect = Some(LogId::new(CommittedLeaderId::new(4, 3), 10));
    vec![
        wire::AdminRequest::AddLearner {
            id: 5,
            addr: "127.0.0.1:5".into(),
            expect,
        },
        wire::AdminRequest::Promote {
            ids: [4].into(),
            expect,
            force: false,
        },
        wire::AdminRequest::Remove {
            id: 3,
            expect,
            force: true,
        },
        wire::AdminRequest::SetAddr {
            id: 2,
            addr: "127.0.0.1:22".into(),
            expect,
            force: false,
        },
    ]
}

fn rejected(h: &Result<(Box<dyn TestIo>, ServerHello), String>, reason: &str) -> bool {
    matches!(h, Ok((_, ServerHello::Rejected { reason: r })) if r == reason)
}

/// P6-T2: StatusEx is answered from the status source before Raft runs and
/// after, needs an accepted hello, and falls back to the durable state for
/// a source that knows no more; without a source it is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_ex_is_answered_before_and_after_raft_runs() {
    let tcp = bind().await;
    let addr = tcp.local_addr().expect("addr").to_string();
    let mut cfg = listener_config(1, &[1, 2, 3], None);
    cfg.status = Some(Arc::new(FixedStatusEx(sample_status_ex())));
    let (listener, slot) =
        ClusterListener::spawn_deferred::<CountingHandler>(tcp, cfg).expect("listener");
    let net = Network::new(net_config(2, [(1, addr.clone())].into(), None));
    assert_eq!(net.status_ex(1).await.expect("before"), sample_status_ex());
    assert_eq!(
        crate::status::StatusTransport::status_ex(&net, 1)
            .await
            .expect("trait"),
        sample_status_ex()
    );
    let stranger = Network::new(net_config(9, [(1, addr.clone())].into(), None));
    let e = stranger.status_ex(1).await.expect_err("unknown peer");
    assert!(matches!(e, ForwardError::Unreachable(_)), "{e:?}");

    let raft = Raft::new(
        1,
        raft_config(),
        Network::new(net_config(1, [(1, addr.clone())].into(), None)),
        MemLog::default(),
        MemSm::default(),
    )
    .await
    .expect("raft");
    assert!(slot.set(raft.clone(), Arc::new(CountingHandler::default())));
    assert_eq!(net.status_ex(1).await.expect("after"), sample_status_ex());
    assert_eq!(
        net.status(1).await.expect("status"),
        sample_status_ex().status
    );
    listener.shutdown().await;
    let _ = raft.shutdown().await;

    // A source with only the durable state (the log store): the default.
    let tcp = bind().await;
    let addr = tcp.local_addr().expect("addr").to_string();
    let durable = sample_status_ex().status;
    let mut cfg = listener_config(1, &[1, 2, 3], None);
    cfg.status = Some(Arc::new(FixedStatus(durable)));
    let (listener, _slot) =
        ClusterListener::spawn_deferred::<CountingHandler>(tcp, cfg).expect("listener");
    let net = Network::new(net_config(2, [(1, addr.clone())].into(), None));
    let got = net.status_ex(1).await.expect("default");
    assert_eq!(got, crate::status::NodeStatusEx::from_status(durable));
    assert_eq!(got.term, 4);
    assert!(!got.raft_running && got.membership.nodes.is_empty());
    listener.shutdown().await;

    let (l, raft, addr) = lone_listener(|_| {}).await;
    let net = Network::new(net_config(2, [(1, addr.to_string())].into(), None));
    let e = net.status_ex(1).await.expect_err("no source");
    assert!(
        matches!(&e, ForwardError::Rejected(m) if m.contains("not served")),
        "{e:?}"
    );
    l.shutdown().await;
    let _ = raft.shutdown().await;
}

/// P6-T2: under mTLS the admin channel takes exactly the `bstk-admin`
/// certificate from the cluster CA; node certificates, certificates from
/// another CA and certificates naming both identities are refused, and the
/// admin certificate is refused as a peer. Membership changes go to the
/// handler (P6-T4) with the admin's address; the listener itself leaves
/// Raft untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_channel_identity_under_mtls() {
    let pki = Pki::new("cluster CA");
    let rogue = Pki::new("rogue CA");
    let (l, raft, handler, addr) = admin_target(Some(&pki.node(1)), true, |_| {}).await;
    let raft = raft.expect("raft");
    let before = raft.metrics().borrow().membership_config.clone();

    let mut io = admin_conn(addr, Some(pki.admin())).await;
    assert_eq!(
        admin_call(&mut io, 1, wire::AdminRequest::Membership).await,
        wire::AdminResponse::Membership(Box::new(sample_status_ex()))
    );
    for (i, req) in (2..).zip(mutating_requests()) {
        assert_eq!(
            admin_call(&mut io, i, req).await,
            wire::AdminResponse::Conflict { current: None }
        );
    }
    let admins = handler.admins.lock().expect("lock").clone();
    assert_eq!(
        admins.iter().map(|(r, _)| r.clone()).collect::<Vec<_>>(),
        mutating_requests()
    );
    assert!(
        admins.iter().all(|(_, a)| a.ip().is_loopback()),
        "{admins:?}"
    );
    assert_eq!(raft.metrics().borrow().membership_config, before);
    // Any node may be asked without naming it; naming another is refused.
    drop(io);
    let (_io, h) = hello_with(addr, Some(pki.admin()), &admin_hello(None))
        .await
        .expect("answer");
    assert!(matches!(h, ServerHello::Accepted { .. }), "{h:?}");
    let h = hello_with(addr, Some(pki.admin()), &admin_hello(Some(2))).await;
    assert!(rejected(&h, REJECT_HELLO), "{:?}", h.as_ref().map(|r| &r.1));

    // A node's certificate on the admin channel.
    let node2 = pki.node(2).client;
    let h = hello_with(addr, Some(node2.clone()), &admin_hello(Some(1))).await;
    assert!(rejected(&h, REJECT_HELLO), "{:?}", h.as_ref().map(|r| &r.1));
    // The admin certificate as a peer (any id).
    for from in [2, 3] {
        let h = hello_with(addr, Some(pki.admin()), &peer_hello(from)).await;
        assert!(rejected(&h, REJECT_HELLO), "{:?}", h.as_ref().map(|r| &r.1));
    }
    // A certificate carrying both names is neither.
    let both = Pki::client_config(
        &pki,
        pki.leaf(&[crate::tls::ADMIN_DNS_NAME.into(), node_dns_name(2)]),
    );
    let h = hello_with(addr, Some(both.clone()), &admin_hello(Some(1))).await;
    assert!(rejected(&h, REJECT_HELLO), "{:?}", h.as_ref().map(|r| &r.1));
    let h = hello_with(addr, Some(both), &peer_hello(2)).await;
    assert!(rejected(&h, REJECT_HELLO), "{:?}", h.as_ref().map(|r| &r.1));
    // An admin certificate from another CA fails the TLS handshake.
    let h = hello_with(addr, Some(rogue.admin()), &admin_hello(Some(1))).await;
    assert!(h.is_err(), "{:?}", h.as_ref().map(|r| &r.1));
    // A plaintext admin hello on a TLS listener.
    let h = hello_with(addr, None, &admin_hello(Some(1))).await;
    assert!(h.is_err(), "{:?}", h.as_ref().map(|r| &r.1));
    // The node certificate still works as the node.
    let (_io, h) = hello_with(addr, Some(node2), &peer_hello(2))
        .await
        .expect("answer");
    assert!(matches!(h, ServerHello::Accepted { .. }), "{h:?}");

    assert_eq!(handler.calls(), 0);
    l.shutdown().await;
    let _ = raft.shutdown().await;
}

/// P6-T5: the operator tool's TLS configuration (`admin_client_tls_from_pem`)
/// reaches a node without knowing its id, trusting only the cluster CA and
/// only node names; a certificate that is not an admin one is refused
/// before any dial.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_tool_tls_accepts_any_node_name_from_the_cluster_ca() {
    use crate::tls::{admin_client_tls_from_pem, verify_node_certificate};
    let pki = Pki::new("cluster CA");
    let rogue = Pki::new("rogue CA");
    let (l, raft, _handler, addr) = admin_target(Some(&pki.node(1)), true, |_| {}).await;
    let raft = raft.expect("raft");
    let tool = |trust: &Pki, signer: &Pki| {
        let (cert, key) = signer.client_leaf(&[crate::tls::ADMIN_DNS_NAME.to_string()]);
        admin_client_tls_from_pem(cert.as_bytes(), key.as_bytes(), trust.ca_pem().as_bytes())
    };
    // The name is a placeholder: the verifier takes the real one from the
    // node's certificate.
    let connect = |cfg: Arc<rustls::ClientConfig>, addr: SocketAddr| async move {
        let tcp = tokio::net::TcpStream::connect(addr).await.expect("tcp");
        let io = tokio_rustls::TlsConnector::from(cfg)
            .connect(
                rustls_pki_types::ServerName::try_from("bstk-cluster").expect("name"),
                tcp,
            )
            .await
            .map_err(|e| e.to_string())?;
        let certs = io.get_ref().1.peer_certificates().map(<[_]>::to_vec);
        Ok::<_, String>((io, certs))
    };

    let cfg = tool(&pki, &pki).expect("config");
    let (mut io, certs) = connect(cfg.clone(), addr).await.expect("handshake");
    verify_node_certificate(certs.as_deref(), 1).expect("node 1's certificate");
    assert!(verify_node_certificate(certs.as_deref(), 2).is_err());
    assert!(verify_node_certificate(None, 1).is_err());
    let f = wire::encode(&admin_hello(None), wire::DEFAULT_MAX_FRAME).expect("encode");
    wire::write_frame(&mut io, &f).await.expect("write");
    match wire::read_frame::<_, ServerMsg>(&mut io, wire::DEFAULT_MAX_FRAME).await {
        Ok(Some(ServerMsg::Hello(ServerHello::Accepted { node_id: 1, .. }))) => {}
        other => panic!("{other:?}"),
    }
    drop(io);

    // A server certificate from another CA is not trusted.
    let tcp = bind().await;
    let rogue_addr = tcp.local_addr().expect("addr");
    let rogue_cfg = listener_config(1, &[1], Some(&tls_with_cert_for(&rogue, &rogue, 1)));
    let (rl, _slot) =
        ClusterListener::spawn_deferred::<CountingHandler>(tcp, rogue_cfg).expect("listener");
    let e = connect(cfg, rogue_addr).await;
    assert!(e.is_err(), "a server from another CA was trusted");
    rl.shutdown().await;

    // A node certificate presented as the tool's is refused locally.
    let (cert, key) = pki.leaf(&[node_dns_name(2)]);
    let e = admin_client_tls_from_pem(cert.as_bytes(), key.as_bytes(), pki.ca_pem().as_bytes())
        .expect_err("a node certificate is not an admin certificate");
    assert!(e.0.contains("not an admin certificate"), "{e}");
    let both = pki.leaf(&[crate::tls::ADMIN_DNS_NAME.into(), node_dns_name(2)]);
    assert!(
        admin_client_tls_from_pem(
            both.0.as_bytes(),
            both.1.as_bytes(),
            pki.ca_pem().as_bytes()
        )
        .is_err()
    );
    // An admin certificate from another CA is refused by the node (TLS 1.3
    // may report it on the first read).
    let wrong_ca = tool(&pki, &rogue).expect("config");
    let refused = match connect(wrong_ca, addr).await {
        Err(_) => true,
        Ok((mut io, _)) => {
            let f = wire::encode(&admin_hello(None), wire::DEFAULT_MAX_FRAME).expect("encode");
            let _ = wire::write_frame(&mut io, &f).await;
            !matches!(
                wire::read_frame::<_, ServerMsg>(&mut io, wire::DEFAULT_MAX_FRAME).await,
                Ok(Some(_))
            )
        }
    };
    assert!(refused, "an admin certificate from another CA was served");
    l.shutdown().await;
    let _ = raft.shutdown().await;
}

#[test]
fn identity_checks_exclude_each_other() {
    use crate::tls::{ADMIN_DNS_NAME, verify_admin_identity, verify_peer_identity};
    let pki = Pki::new("cluster CA");
    let der = |(cert, _): (String, String)| {
        rustls_pki_types::CertificateDer::pem_slice_iter(cert.as_bytes())
            .collect::<Result<Vec<_>, _>>()
            .expect("certs")
    };
    let admin = der(pki.client_leaf(&[ADMIN_DNS_NAME.into()]));
    let node = der(pki.leaf(&[node_dns_name(2)]));
    let both = der(pki.leaf(&[ADMIN_DNS_NAME.into(), node_dns_name(2)]));
    let other = der(pki.client_leaf(&["bstk-admin.example".into()]));
    assert_eq!(verify_admin_identity(Some(&admin)), Ok(()));
    assert!(verify_admin_identity(Some(&node)).is_err());
    assert!(verify_admin_identity(Some(&both)).is_err());
    assert!(verify_admin_identity(Some(&other)).is_err());
    assert!(verify_admin_identity(None).is_err());
    assert_eq!(verify_peer_identity(Some(&node), 2), Ok(()));
    assert!(verify_peer_identity(Some(&admin), 2).is_err());
    assert!(verify_peer_identity(Some(&both), 2).is_err());
}

#[test]
fn plaintext_admin_only_from_loopback() {
    use crate::listener::plaintext_admin_allowed;
    for ok in ["127.0.0.1", "127.9.9.9", "::1", "::ffff:127.0.0.1"] {
        assert!(plaintext_admin_allowed(ok.parse().expect("ip")), "{ok}");
    }
    for no in [
        "10.0.0.1",
        "0.0.0.0",
        "::",
        "::ffff:10.0.0.1",
        "fe80::1",
        "192.168.1.1",
    ] {
        assert!(!plaintext_admin_allowed(no.parse().expect("ip")), "{no}");
    }
}

/// P6-T2: in plaintext mode an admin tool on loopback is accepted (and its
/// membership read is served before Raft runs too). P6-T4: before Raft runs
/// a change answers `NotLeader` without a leader; with a handler that does
/// not implement changes, `Unsupported`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plaintext_admin_from_loopback_is_accepted() {
    let (l, _, _, addr) = admin_target(None, false, |_| {}).await;
    let mut io = admin_conn(addr, None).await;
    assert_eq!(
        admin_call(&mut io, 1, wire::AdminRequest::Membership).await,
        wire::AdminResponse::Membership(Box::new(sample_status_ex()))
    );
    for (i, req) in (2..).zip(mutating_requests()) {
        assert_eq!(
            admin_call(&mut io, i, req).await,
            wire::AdminResponse::NotLeader {
                leader: None,
                addr: None
            }
        );
    }
    l.shutdown().await;

    let tcp = bind().await;
    let addr = tcp.local_addr().expect("addr");
    let (l, slot) =
        ClusterListener::spawn_deferred::<PlainHandler>(tcp, listener_config(1, &[1, 2, 3], None))
            .expect("listener");
    let net = Network::new(net_config(1, [(1, addr.to_string())].into(), None));
    let raft = Raft::new(1, raft_config(), net, MemLog::default(), MemSm::default())
        .await
        .expect("raft");
    assert!(slot.set(raft.clone(), Arc::new(PlainHandler)));
    let mut io = admin_conn(addr, None).await;
    for (i, req) in (1..).zip(mutating_requests()) {
        assert_eq!(
            admin_call(&mut io, i, req).await,
            wire::AdminResponse::Unsupported
        );
    }
    l.shutdown().await;
    let _ = raft.shutdown().await;

    // Without a status source the read is refused, not failed.
    let (l, raft, addr) = lone_listener(|_| {}).await;
    let mut io = admin_conn(addr, None).await;
    assert!(matches!(
        admin_call(&mut io, 1, wire::AdminRequest::Membership).await,
        wire::AdminResponse::Refused { .. }
    ));
    l.shutdown().await;
    let _ = raft.shutdown().await;
}

/// P6-T2: an admin connection carries nothing but admin requests (no
/// forwards, Raft RPCs, controls, status probes or hellos), and a peer
/// connection no admin request: either closes the connection before
/// anything is served.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_and_peer_channels_do_not_mix() {
    let (l, raft, handler, addr) = admin_target(None, true, |_| {}).await;
    let raft = raft.expect("raft");
    let vote_before = raft.metrics().borrow().vote;
    let requests = [
        wire::RpcRequest::Forward(forward_from(2)),
        wire::RpcRequest::Vote(vote_req(9, 2)),
        wire::RpcRequest::AppendEntries(heartbeat()),
        wire::RpcRequest::Control(ControlRequest {
            from: 2,
            op: Op::SetDraining(true),
        }),
        wire::RpcRequest::Status,
        wire::RpcRequest::StatusEx,
    ];
    for body in requests {
        let mut io = admin_conn(addr, None).await;
        let got = exchange(&mut io, &ClientMsg::Request { id: 1, body }).await;
        assert!(got.is_none(), "answered on an admin connection: {got:?}");
    }
    for hello in [admin_hello(Some(1)), peer_hello(2)] {
        let mut io = admin_conn(addr, None).await;
        assert!(exchange(&mut io, &hello).await.is_none());
    }
    // An admin request above the admin frame limit.
    let mut io = admin_conn(addr, None).await;
    let big = wire::AdminRequest::AddLearner {
        id: 5,
        addr: "a".repeat(wire::MAX_NODE_ADDR_LEN),
        expect: None,
    };
    let mut f = wire::encode(
        &ClientMsg::Admin { id: 1, body: big },
        wire::DEFAULT_MAX_FRAME,
    )
    .expect("encode");
    f[..4].copy_from_slice(&(wire::ADMIN_MAX_REQUEST_FRAME as u32 + 1).to_be_bytes());
    let _ = io.write_all(&f).await;
    let r = tokio::time::timeout(
        Duration::from_secs(2),
        wire::read_frame::<_, ServerMsg>(&mut io, wire::DEFAULT_MAX_FRAME),
    )
    .await
    .expect("closed in time");
    assert!(!matches!(r, Ok(Some(_))), "{r:?}");

    // A peer sending an admin request.
    for body in [
        wire::AdminRequest::Membership,
        mutating_requests().remove(0),
    ] {
        let (mut io, h) = hello_with(addr, None, &peer_hello(2))
            .await
            .expect("answer");
        assert!(matches!(h, ServerHello::Accepted { .. }), "{h:?}");
        assert!(
            exchange(&mut io, &ClientMsg::Admin { id: 1, body })
                .await
                .is_none()
        );
    }
    // An admin request before any hello.
    let tcp = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let mut io: Box<dyn TestIo> = Box::new(tcp);
    assert!(
        exchange(
            &mut io,
            &ClientMsg::Admin {
                id: 1,
                body: wire::AdminRequest::Membership
            }
        )
        .await
        .is_none()
    );

    assert_eq!(handler.calls(), 0);
    assert!(handler.controls.lock().expect("lock").is_empty());
    assert_eq!(raft.metrics().borrow().vote, vote_before);
    l.shutdown().await;
    let _ = raft.shutdown().await;
}

/// P6-T2: admin connections have their own small budget (beyond it the
/// hello is refused with a reason) and close when idle; they never take a
/// peer's slot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_connections_are_limited_and_time_out() {
    let (l, _, _, addr) = admin_target(None, false, |c| {
        c.max_admin_conns = 1;
        c.admin_idle_timeout = Duration::from_millis(300);
    })
    .await;
    let mut first = admin_conn(addr, None).await;
    let h = hello_with(addr, None, &admin_hello(None)).await;
    assert!(
        rejected(&h, crate::listener::REJECT_ADMIN_BUSY),
        "{:?}",
        h.as_ref().map(|r| &r.1)
    );
    // Peers are unaffected.
    let mut peer = raw_hello(addr, 2).await;
    // Requests keep it open past the idle timeout; silence closes it.
    for i in 0..3 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(matches!(
            admin_call(&mut first, i, wire::AdminRequest::Membership).await,
            wire::AdminResponse::Membership(_)
        ));
    }
    let mut buf = [0u8; 16];
    let n = tokio::time::timeout(Duration::from_secs(2), first.read(&mut buf))
        .await
        .expect("closed in time");
    assert!(matches!(n, Ok(0) | Err(_)), "{n:?}");
    // The slot is free again.
    let _again = admin_conn(addr, None).await;
    assert!(!closes_within(&mut peer, Duration::from_millis(100)).await);
    l.shutdown().await;
}

/// Whether `s` closes within `d` (false: still open).
async fn closes_within(s: &mut tokio::net::TcpStream, d: Duration) -> bool {
    let mut buf = [0u8; 16];
    matches!(
        tokio::time::timeout(d, s.read(&mut buf)).await,
        Ok(Ok(0) | Err(_))
    )
}

fn probe_hello(from: NodeId) -> ClientMsg {
    ClientMsg::ProbeHello(Hello {
        version: PROTOCOL_VERSION,
        from,
        to: 1,
        max_job_size: bstk_proto::DEFAULT_MAX_JOB_SIZE,
    })
}

/// P6-T3: a node that is not a member (here 9: the listener allows 1..=3)
/// is refused as a peer but answered on a probe connection, which carries
/// status probes only and is bounded by `max_probe_conns`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn probe_connections_answer_non_members_status_only() {
    let (l, raft, handler, addr) = admin_target(None, true, |c| c.max_probe_conns = 2).await;
    let stranger = Network::new(net_config(9, [(1, addr.to_string())].into(), None));
    let e = stranger.status_ex(1).await.expect_err("not a member");
    assert!(
        matches!(&e, ForwardError::Unreachable(m) if m.contains("not a member")),
        "{e:?}"
    );
    assert_eq!(
        stranger.probe_status_ex(1).await.expect("probe"),
        sample_status_ex()
    );
    assert_eq!(
        crate::status::StatusTransport::status_ex(&stranger, 1)
            .await
            .expect("trait"),
        sample_status_ex()
    );
    // Status probes only: anything else closes the connection.
    let (mut io, h) = hello_with(addr, None, &probe_hello(9))
        .await
        .expect("hello");
    assert!(
        matches!(h, ServerHello::Accepted { node_id: 1, .. }),
        "{h:?}"
    );
    let ok = exchange(
        &mut io,
        &ClientMsg::Request {
            id: 1,
            body: wire::RpcRequest::Status,
        },
    )
    .await;
    match ok {
        Some(ServerMsg::Response { id: 1, body }) => assert!(body.into_status().is_some()),
        other => panic!("status expected: {other:?}"),
    }
    let forward = ClientMsg::Request {
        id: 2,
        body: wire::RpcRequest::Forward(ForwardRequest {
            from: 9,
            items: Vec::new(),
        }),
    };
    assert!(exchange(&mut io, &forward).await.is_none());
    let (mut io, _) = hello_with(addr, None, &probe_hello(9))
        .await
        .expect("hello");
    assert!(exchange(&mut io, &admin_hello(Some(1))).await.is_none());
    // Misaddressed, or claiming the listener's own id.
    let wrong = ClientMsg::ProbeHello(Hello {
        version: PROTOCOL_VERSION,
        from: 9,
        to: 2,
        max_job_size: 0,
    });
    assert!(rejected(
        &hello_with(addr, None, &wrong).await,
        REJECT_HELLO
    ));
    assert!(rejected(
        &hello_with(addr, None, &probe_hello(1)).await,
        REJECT_HELLO
    ));
    // At most `max_probe_conns` at once.
    let (_a, _) = hello_with(addr, None, &probe_hello(9)).await.expect("1");
    let (_b, _) = hello_with(addr, None, &probe_hello(8)).await.expect("2");
    let h = hello_with(addr, None, &probe_hello(7)).await;
    assert!(
        rejected(&h, crate::listener::REJECT_PROBE_BUSY),
        "{:?}",
        h.as_ref().map(|r| &r.1)
    );
    assert_eq!(handler.calls(), 0);
    drop((_a, _b));
    l.shutdown().await;
    if let Some(r) = raft {
        let _ = r.shutdown().await;
    }
}

/// P6-T3: under mTLS a probe needs the certificate of the id it claims (any
/// id, member or not); the admin certificate is no prober.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn probe_connections_check_identity_under_mtls() {
    let pki = Pki::new("cluster CA");
    let (l, _, _, addr) = admin_target(Some(&pki.node(1)), false, |_| {}).await;
    // Node 9's certificate, not a member: answered over a probe connection.
    let tls9 = pki.node(9);
    let net9 = Network::new(net_config(9, [(1, addr.to_string())].into(), Some(&tls9)));
    assert_eq!(
        net9.probe_status_ex(1).await.expect("probe"),
        sample_status_ex()
    );
    // Node 2's certificate claiming id 9.
    let h = hello_with(addr, Some(pki.node(2).client), &probe_hello(9)).await;
    assert!(rejected(&h, REJECT_HELLO), "{:?}", h.as_ref().map(|r| &r.1));
    // The admin certificate.
    let h = hello_with(addr, Some(pki.admin()), &probe_hello(9)).await;
    assert!(rejected(&h, REJECT_HELLO), "{:?}", h.as_ref().map(|r| &r.1));
    // Plaintext on a TLS listener.
    assert!(hello_with(addr, None, &probe_hello(9)).await.is_err());
    l.shutdown().await;
}

fn status_probe(id: u64) -> ClientMsg {
    ClientMsg::Request {
        id,
        body: wire::RpcRequest::Status,
    }
}

/// Whether `io` answers a status probe (`false`: the listener closed it).
async fn probe_answered(io: &mut Box<dyn TestIo>, id: u64) -> bool {
    matches!(
        exchange(io, &status_probe(id)).await,
        Some(ServerMsg::Response { id: got, .. }) if got == id
    )
}

/// P6-T3 review (M2): one probe connection per node id (a newer one takes
/// over the older's slot, even when every slot is taken), so a certificate
/// holder cannot hold more than one; each closes after its request cap or
/// its lifetime, however busy.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn probe_connections_are_one_per_node_and_capped() {
    let (l, _, _, addr) = admin_target(None, false, |c| {
        c.max_probe_conns = 2;
        c.probe_max_requests = 3;
        c.probe_max_lifetime = Duration::from_millis(600);
    })
    .await;
    let (mut a, _) = hello_with(addr, None, &probe_hello(9)).await.expect("9");
    let (mut b, _) = hello_with(addr, None, &probe_hello(8)).await.expect("8");
    assert!(probe_answered(&mut a, 1).await);
    // Every slot is taken: node 9's newer connection replaces its older
    // one, node 7 is still refused.
    let (mut a2, h) = hello_with(addr, None, &probe_hello(9))
        .await
        .expect("9 again");
    assert!(matches!(h, ServerHello::Accepted { .. }), "{h:?}");
    assert!(
        !probe_answered(&mut a, 2).await,
        "the older probe stays open"
    );
    assert!(probe_answered(&mut a2, 1).await);
    let h = hello_with(addr, None, &probe_hello(7)).await;
    assert!(
        rejected(&h, crate::listener::REJECT_PROBE_BUSY),
        "{:?}",
        h.as_ref().map(|r| &r.1)
    );
    // The request cap: three probes, then the connection closes.
    assert!(probe_answered(&mut b, 1).await);
    assert!(probe_answered(&mut b, 2).await);
    assert!(probe_answered(&mut b, 3).await);
    assert!(!probe_answered(&mut b, 4).await, "past the request cap");
    // Its slot is free again.
    let (mut c, _) = hello_with(addr, None, &probe_hello(7)).await.expect("7");
    // The lifetime cap: a request every 400 ms (well below the idle
    // timeout and the request cap), closed after 600 ms anyway.
    assert!(probe_answered(&mut c, 1).await);
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(probe_answered(&mut c, 2).await);
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(!probe_answered(&mut c, 3).await, "past the lifetime cap");
    l.shutdown().await;
}

/// P6-T3 review (M2): plaintext proves no identity, so a plaintext probe
/// hello is accepted from loopback only, unless remote plaintext was
/// allowed explicitly.
#[test]
fn plaintext_probes_are_loopback_only_by_default() {
    use crate::listener::plaintext_probe_allowed;
    let lan: std::net::IpAddr = "10.1.2.3".parse().expect("ip");
    let mapped: std::net::IpAddr = "::ffff:127.0.0.1".parse().expect("ip");
    assert!(!plaintext_probe_allowed(lan, false));
    assert!(plaintext_probe_allowed(lan, true));
    for ip in ["127.0.0.1", "::1"] {
        assert!(plaintext_probe_allowed(ip.parse().expect("ip"), false));
    }
    assert!(plaintext_probe_allowed(mapped, false));
}
