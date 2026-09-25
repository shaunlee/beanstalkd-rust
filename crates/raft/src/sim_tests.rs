//! Tests of the simulated network and the in-process cluster harness.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bstk_engine::EngineInput;

use crate::forward::{ForwardError, ForwardHandler, ForwardTransport};
use crate::sim::{FaultKind, FaultSchedule, SimCluster, SimConfig, SimNetwork};
use crate::test_store::{MemLog, MemSm};
use crate::{ForwardRequest, ForwardResponse, NodeId, Op, Request, conn_id};

type Sms = Arc<Mutex<BTreeMap<NodeId, MemSm>>>;

fn config() -> openraft::Config {
    openraft::Config {
        heartbeat_interval: 50,
        election_timeout_min: 150,
        election_timeout_max: 300,
        ..Default::default()
    }
}

fn req(now: u64) -> Request {
    Request { now, op: Op::Tick }
}

/// Starts nodes 1..=n over `net` with in-memory storage; returns the
/// cluster and the state machines (for inspection).
async fn start(n: u64, net: SimNetwork, cfg: openraft::Config) -> (SimCluster, Sms) {
    let sms: Sms = Arc::default();
    let ids: Vec<NodeId> = (1..=n).collect();
    let s = sms.clone();
    let cluster = SimCluster::start(&ids, cfg, net, move |id| {
        let sm = MemSm::default();
        s.lock().expect("lock").insert(id, sm.clone());
        async move { (MemLog::default(), sm) }
    })
    .await
    .expect("start");
    cluster.initialize().await.expect("initialize");
    (cluster, sms)
}

fn applied(sms: &Sms, id: NodeId) -> Vec<Request> {
    sms.lock().expect("lock")[&id].applied()
}

/// Writes through the current leader among `among`, retrying until
/// `deadline`; returns the log index.
async fn write(c: &SimCluster, among: &[NodeId], r: Request, within: Duration) -> u64 {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(!left.is_zero(), "write {r:?} did not succeed");
        let Ok(l) = c.wait_for_leader(among, left).await else {
            continue;
        };
        let raft = c.raft(l).expect("leader running");
        if let Ok(Ok(resp)) =
            tokio::time::timeout(Duration::from_secs(1), raft.client_write(r.clone())).await
        {
            return resp.log_id.index;
        }
    }
}

async fn assert_converged(c: &SimCluster, sms: &Sms, among: &[NodeId], index: u64) {
    c.wait_applied(among, index, Duration::from_secs(20))
        .await
        .expect("applied");
    let want = applied(sms, among[0]);
    for &id in among {
        assert_eq!(applied(sms, id), want, "node {id} diverged");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sim_three_nodes_elect_and_replicate() {
    let (c, sms) = start(3, SimNetwork::new(1, SimConfig::default()), config()).await;
    let all = c.ids();
    let mut last = 0;
    for i in 0..20 {
        last = write(&c, &all, req(i), Duration::from_secs(10)).await;
    }
    assert_converged(&c, &sms, &all, last).await;
    assert_eq!(applied(&sms, 1), (0..20).map(req).collect::<Vec<_>>());
    c.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sim_partitioned_leader_is_replaced_and_cannot_commit() {
    let net = SimNetwork::new(2, SimConfig::default());
    let (c, sms) = start(3, net.clone(), config()).await;
    let all = c.ids();
    for i in 0..5 {
        write(&c, &all, req(i), Duration::from_secs(10)).await;
    }
    let old = c
        .wait_for_leader(&all, Duration::from_secs(10))
        .await
        .expect("leader");

    net.isolate(old, &all);
    let rest: Vec<NodeId> = all.iter().copied().filter(|&i| i != old).collect();
    let new = c
        .wait_for_leader(&rest, Duration::from_secs(10))
        .await
        .expect("new leader");
    assert_ne!(new, old);

    // The old leader cannot commit.
    let stale = Request {
        now: 999,
        op: Op::SetDraining(true),
    };
    let r = tokio::time::timeout(
        Duration::from_millis(800),
        c.raft(old).expect("old").client_write(stale.clone()),
    )
    .await;
    assert!(!matches!(r, Ok(Ok(_))), "old leader committed: {r:?}");
    assert!(!applied(&sms, old).contains(&stale));

    // The majority keeps committing.
    let idx = write(&c, &rest, req(100), Duration::from_secs(10)).await;
    assert_converged(&c, &sms, &rest, idx).await;

    // Heal: everyone converges on the majority's log.
    net.heal();
    let idx = write(&c, &all, req(101), Duration::from_secs(10)).await;
    assert_converged(&c, &sms, &all, idx).await;
    for &id in &all {
        assert!(!applied(&sms, id).contains(&stale), "node {id}");
    }
    c.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sim_asymmetric_partition_of_leader() {
    let net = SimNetwork::new(3, SimConfig::default());
    let (c, sms) = start(3, net.clone(), config()).await;
    let all = c.ids();
    write(&c, &all, req(0), Duration::from_secs(10)).await;
    let old = c
        .wait_for_leader(&all, Duration::from_secs(10))
        .await
        .expect("leader");
    // The leader can receive but not send.
    for &o in &all {
        if o != old {
            net.block(old, o);
        }
    }
    let rest: Vec<NodeId> = all.iter().copied().filter(|&i| i != old).collect();
    let new = c
        .wait_for_leader(&rest, Duration::from_secs(10))
        .await
        .expect("new leader");
    assert_ne!(new, old);
    let idx = write(&c, &rest, req(1), Duration::from_secs(10)).await;
    assert_converged(&c, &sms, &rest, idx).await;
    assert!(
        net.fault_log()
            .iter()
            .any(|e| e.from == old && e.kind == FaultKind::Blocked)
    );
    net.heal();
    let idx = write(&c, &all, req(2), Duration::from_secs(10)).await;
    assert_converged(&c, &sms, &all, idx).await;
    c.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sim_paused_leader_is_replaced_and_rejoins() {
    let net = SimNetwork::new(4, SimConfig::default());
    let (c, sms) = start(3, net.clone(), config()).await;
    let all = c.ids();
    write(&c, &all, req(0), Duration::from_secs(10)).await;
    let old = c
        .wait_for_leader(&all, Duration::from_secs(10))
        .await
        .expect("leader");
    net.pause(old);
    let rest: Vec<NodeId> = all.iter().copied().filter(|&i| i != old).collect();
    let new = c
        .wait_for_leader(&rest, Duration::from_secs(10))
        .await
        .expect("new leader");
    assert_ne!(new, old);
    let idx = write(&c, &rest, req(1), Duration::from_secs(10)).await;
    net.resume(old);
    assert_converged(&c, &sms, &all, idx).await;
    c.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sim_lossy_network_with_fault_schedule_converges() {
    let net = SimNetwork::new(5, SimConfig::default());
    let (c, sms) = start(3, net.clone(), config()).await;
    let all = c.ids();
    write(&c, &all, req(0), Duration::from_secs(10)).await;
    net.set_drop(0.05);
    net.set_duplicate(0.1);
    net.set_delay(Duration::ZERO, Duration::from_millis(3));
    let schedule = FaultSchedule::generate(net.seed(), &all, 12, Duration::from_millis(80));
    let runner = {
        let (s, n) = (schedule.clone(), net.clone());
        tokio::spawn(async move { s.run(&n).await })
    };
    // Writes during the faults may or may not succeed.
    for i in 1..30 {
        let raft_ids = all.clone();
        if let Ok(l) = c
            .wait_for_leader(&raft_ids, Duration::from_millis(200))
            .await
        {
            let raft = c.raft(l).expect("leader");
            let _ =
                tokio::time::timeout(Duration::from_millis(300), raft.client_write(req(i))).await;
        }
    }
    runner.await.expect("schedule");
    net.set_drop(0.0);
    net.set_duplicate(0.0);
    net.set_delay(Duration::ZERO, Duration::ZERO);
    net.heal();
    let idx = write(&c, &all, req(1000), Duration::from_secs(20)).await;
    assert_converged(&c, &sms, &all, idx).await;
    let log = net.fault_log();
    assert!(
        log.iter().any(|e| e.kind == FaultKind::Duplicate),
        "no duplicates injected"
    );
    c.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sim_wiped_node_catches_up_by_snapshot() {
    let mut cfg = config();
    cfg.snapshot_max_chunk_size = 32;
    cfg.max_in_snapshot_log_to_keep = 0;
    cfg.purge_batch_size = 1;
    let net = SimNetwork::new(6, SimConfig::default());
    let (mut c, sms) = start(3, net.clone(), cfg).await;
    let all = c.ids();
    for i in 0..10 {
        write(&c, &all, req(i), Duration::from_secs(10)).await;
    }
    let leader = c
        .wait_for_leader(&all, Duration::from_secs(10))
        .await
        .expect("leader");
    let victim = *all.iter().find(|&&i| i != leader).expect("follower");
    c.stop_node(victim).await;
    let rest: Vec<NodeId> = all.iter().copied().filter(|&i| i != victim).collect();
    let mut idx = 0;
    for i in 10..20 {
        idx = write(&c, &rest, req(i), Duration::from_secs(10)).await;
    }
    let l = c.raft(leader).expect("leader");
    l.trigger().snapshot().await.expect("snapshot");
    let applied_id = l
        .wait(Some(Duration::from_secs(5)))
        .applied_index_at_least(Some(idx), "applied")
        .await
        .expect("applied")
        .last_applied
        .expect("applied");
    l.wait(Some(Duration::from_secs(5)))
        .snapshot(applied_id, "snapshot")
        .await
        .expect("snapshot built");
    l.trigger()
        .purge_log(applied_id.index)
        .await
        .expect("purge");
    l.wait(Some(Duration::from_secs(5)))
        .purged(Some(applied_id), "purged")
        .await
        .expect("purged");

    let sm = MemSm::default();
    sms.lock().expect("lock").insert(victim, sm.clone());
    c.start_node(victim, MemLog::default(), sm)
        .await
        .expect("restart");
    assert_converged(&c, &sms, &all, applied_id.index).await;
    let snap = c.raft(victim).expect("victim").metrics().borrow().snapshot;
    assert!(
        snap.is_some_and(|s| s.index >= applied_id.index),
        "{snap:?}"
    );
    c.shutdown().await;
}

#[derive(Default)]
struct Recorder {
    calls: AtomicUsize,
}

impl ForwardHandler for Recorder {
    async fn forward(&self, _req: ForwardRequest) -> ForwardResponse {
        self.calls.fetch_add(1, Ordering::SeqCst);
        ForwardResponse::Accepted
    }
}

fn forward_from(from: NodeId) -> ForwardRequest {
    let conn = conn_id(from, 1);
    ForwardRequest {
        from,
        items: vec![(conn, 1, EngineInput::Connect(conn))],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sim_forwarding() {
    let net = SimNetwork::new(7, SimConfig::default());
    let (c, _sms) = start(3, net.clone(), config()).await;
    let rec = Arc::new(Recorder::default());
    c.set_forward_handler(1, rec.clone());
    let n2 = net.node(2);

    assert_eq!(
        n2.forward(1, forward_from(2)).await,
        Ok(ForwardResponse::Accepted)
    );
    assert_eq!(rec.calls.load(Ordering::SeqCst), 1);
    // Without a handler: NotLeader.
    assert_eq!(
        n2.forward(3, forward_from(2)).await,
        Ok(ForwardResponse::NotLeader { leader: None })
    );
    // The same ownership checks as the TCP listener.
    let e = n2.forward(1, forward_from(3)).await.expect_err("rejected");
    assert!(matches!(e, ForwardError::Rejected(_)), "{e:?}");
    // A blocked link is unreachable; a lost response times out after the
    // request took effect.
    net.block(2, 1);
    let e = n2.forward(1, forward_from(2)).await.expect_err("blocked");
    assert!(matches!(e, ForwardError::Unreachable(_)), "{e:?}");
    net.unblock(2, 1);
    net.block(1, 2);
    let e = n2
        .forward(1, forward_from(2))
        .await
        .expect_err("lost reply");
    assert_eq!(e, ForwardError::Timeout);
    assert_eq!(rec.calls.load(Ordering::SeqCst), 2);
    net.heal();
    // Duplication delivers twice.
    net.set_duplicate(1.0);
    n2.forward(1, forward_from(2)).await.expect("forward");
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(rec.calls.load(Ordering::SeqCst), 4);
    net.set_duplicate(0.0);
    // A paused target stalls until resumed.
    net.pause(1);
    let pending = {
        let n2 = n2.clone();
        tokio::spawn(async move { n2.forward(1, forward_from(2)).await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(rec.calls.load(Ordering::SeqCst), 4);
    net.resume(1);
    assert_eq!(pending.await.expect("join"), Ok(ForwardResponse::Accepted));
    assert_eq!(rec.calls.load(Ordering::SeqCst), 5);
    c.shutdown().await;
}

fn faulty(seed: u64) -> SimNetwork {
    let n = SimNetwork::new(seed, SimConfig::default());
    n.set_drop(0.2);
    n.set_duplicate(0.2);
    n.set_delay(Duration::ZERO, Duration::from_millis(10));
    n
}

#[test]
fn sim_same_seed_same_faults() {
    let links = [(1, 2), (2, 1), (1, 3), (3, 1), (2, 3), (3, 2)];
    let (a, b, other) = (faulty(42), faulty(42), faulty(43));
    // Different interleavings of the links give the same per-link faults.
    for _ in 0..200 {
        for &(f, t) in &links {
            a.next_decision(f, t);
            other.next_decision(f, t);
        }
        for &(f, t) in links.iter().rev() {
            b.next_decision(f, t);
        }
    }
    let mut differs = false;
    for &(f, t) in &links {
        let la = a.link_fault_log(f, t);
        assert!(!la.is_empty());
        assert_eq!(la, b.link_fault_log(f, t), "link {f}->{t}");
        differs |= la != other.link_fault_log(f, t);
    }
    assert!(differs, "another seed gave the same faults");

    // Schedules too.
    let s1 = FaultSchedule::generate(9, &[1, 2, 3], 50, Duration::from_millis(100));
    let s2 = FaultSchedule::generate(9, &[1, 2, 3], 50, Duration::from_millis(100));
    let s3 = FaultSchedule::generate(10, &[1, 2, 3], 50, Duration::from_millis(100));
    assert_eq!(s1, s2);
    assert_ne!(s1, s3);
    assert_eq!(s1.steps.len(), 51);
}
