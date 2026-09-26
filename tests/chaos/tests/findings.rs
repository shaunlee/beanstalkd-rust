//! Minimal reproductions of the problems the chaos runs found, on the
//! real storage over the simulated network (`SimCluster`), kept as
//! regression tests now that they are fixed.

#![allow(clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use bstk_engine::{ConnId, EngineConfig, EngineInput, StaticSysInfo};
use bstk_proto::Response;
use bstk_raft::listener::VoteGate;
use bstk_raft::sim::{SimCluster, SimConfig, SimNetwork};
use bstk_raft::status::{self, Adopt};
use bstk_raft::storage::{
    self, ClusterStateMachine, LogOptions, LogStore, ReplySink, SmOptions, StateHandle,
};
use bstk_raft::{NodeId, Op, Request, conn_id};

struct NoSink;

impl ReplySink for NoSink {
    fn applied(&self, _: ConnId, _: u64) {}
    fn deliver(&self, _: ConnId, _: Response) {}
    fn closed(&self, _: ConnId) {}
}

fn open(dir: &Path, id: NodeId) -> (LogStore, ClusterStateMachine) {
    let opts = SmOptions {
        node_id: id,
        engine: EngineConfig::default(),
        sys: Arc::new(|| Box::new(StaticSysInfo::default())),
        sink: Arc::new(NoSink),
    };
    storage::open(dir, LogOptions::default(), opts).unwrap()
}

fn config() -> openraft::Config {
    openraft::Config {
        heartbeat_interval: 50,
        election_timeout_min: 150,
        election_timeout_max: 300,
        ..Default::default()
    }
}

struct Fixture {
    _tmp: tempfile::TempDir,
    dirs: BTreeMap<NodeId, PathBuf>,
    handles: BTreeMap<NodeId, StateHandle>,
    cluster: SimCluster,
    net: SimNetwork,
}

async fn start3() -> Fixture {
    let tmp = tempfile::tempdir().unwrap();
    let dirs: BTreeMap<NodeId, PathBuf> = (1..=3)
        .map(|i| (i, tmp.path().join(format!("n{i}"))))
        .collect();
    let net = SimNetwork::new(7, SimConfig::default());
    let mut handles = BTreeMap::new();
    let mut opened = BTreeMap::new();
    for (&id, d) in &dirs {
        let (log, sm) = open(d, id);
        // Status probes are answered from the log store (until the node is
        // stopped: `unregister` drops it, releasing the directory lock).
        net.set_status_source(id, Some(Arc::new(log.clone())));
        handles.insert(id, sm.handle());
        opened.insert(id, (log, sm));
    }
    let cluster = SimCluster::start(&[1, 2, 3], config(), net.clone(), |id| {
        let s = opened.remove(&id).unwrap();
        async move { s }
    })
    .await
    .unwrap();
    cluster.initialize().await.unwrap();
    Fixture {
        _tmp: tmp,
        dirs,
        handles,
        cluster,
        net,
    }
}

async fn write(c: &SimCluster, among: &[NodeId], op: Op) -> u64 {
    for _ in 0..100 {
        if let Ok(l) = c.wait_for_leader(among, Duration::from_secs(5)).await {
            let r = tokio::time::timeout(
                Duration::from_secs(1),
                c.raft(l).unwrap().client_write(Request {
                    now: 1,
                    op: op.clone(),
                }),
            )
            .await;
            if let Ok(Ok(resp)) = r {
                return resp.log_id.index;
            }
        }
    }
    panic!("write {op:?} failed");
}

fn wipe(dir: &Path) {
    std::fs::remove_dir_all(dir).unwrap();
}

/// Finding 1 (fixed by enabling openraft's `loosen-follower-log-revert`
/// feature): a follower that restarts with an empty data directory while
/// the leader still remembers how far it had replicated to it made the
/// leader's Raft core panic ("follower log reversion is not allowed
/// without `--features loosen-follower-log-revert`"). The leader then
/// kept reporting itself as leader but served nothing. With the feature
/// the leader resets its view of the follower and replicates again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wiped_follower_rejoining_panics_the_leader() {
    let mut f = start3().await;
    let all = [1, 2, 3];
    for _ in 0..10 {
        write(&f.cluster, &all, Op::Tick).await;
    }
    let leader = f
        .cluster
        .wait_for_leader(&all, Duration::from_secs(5))
        .await
        .unwrap();
    let follower = all.into_iter().find(|&i| i != leader).unwrap();
    f.cluster.stop_node(follower).await;
    wipe(&f.dirs[&follower]);
    let (log, sm) = open(&f.dirs[&follower], follower);
    f.cluster.start_node(follower, log, sm).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let r = tokio::time::timeout(
        Duration::from_secs(2),
        f.cluster.raft(leader).unwrap().client_write(Request {
            now: 1,
            op: Op::Tick,
        }),
    )
    .await;
    assert!(
        matches!(r, Ok(Ok(_))),
        "the leader (node {leader}) no longer serves after node {follower} rejoined wiped: {r:?}"
    );
}

/// Finding 2 (fixed by rejoin mode): a committed entry was lost when a
/// node that acknowledged it rejoined with an empty data directory and
/// then voted. The entry (a `Connect` of connection X) is committed on the
/// leader and one follower (the "acker") while the third node is
/// partitioned away; the acker is wiped and restarted, the leader dies and
/// the partition heals. Without rejoin mode the two remaining nodes elected
/// the node that never had the entry. With it (as the server does: elections
/// off and the vote gate closed until the node has applied an index learned
/// from a leader), no leader can be elected until the old leader is back,
/// and the entry survives on every node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wiped_voter_in_rejoin_mode_keeps_committed_entries() {
    let mut f = start3().await;
    let all = [1, 2, 3];
    for _ in 0..5 {
        write(&f.cluster, &all, Op::Tick).await;
    }
    let leader = f
        .cluster
        .wait_for_leader(&all, Duration::from_secs(5))
        .await
        .unwrap();
    let others: Vec<NodeId> = all.into_iter().filter(|&i| i != leader).collect();
    let (behind, acker) = (others[0], others[1]);
    f.net.isolate(behind, &all);
    let x = conn_id(1, 777);
    let idx = write(
        &f.cluster,
        &[leader, acker],
        Op::Conn {
            seq: 1,
            input: EngineInput::Connect(x),
        },
    )
    .await;
    eprintln!("connect of {x} committed at index {idx} on nodes {leader} and {acker}");

    // The acker loses its data and restarts in rejoin mode.
    f.cluster.stop_node(acker).await;
    wipe(&f.dirs[&acker]);
    let gate = Arc::new(VoteGate::new(false));
    f.net.set_vote_gate(acker, Some(gate.clone()));
    let (log, sm) = open(&f.dirs[&acker], acker);
    f.handles.insert(acker, sm.handle());
    f.cluster.start_node(acker, log, sm).await.unwrap();
    f.cluster.raft(acker).unwrap().runtime_config().elect(false);
    f.cluster.stop_node(leader).await;
    f.net.heal();

    // Without the old leader, nobody may lead: the node lacking the entry
    // gets no vote from the rejoining acker, which does not stand itself.
    let rest = [behind, acker];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        for id in rest {
            let m = f.cluster.raft(id).unwrap().metrics().borrow().clone();
            assert!(
                !m.state.is_leader(),
                "node {id} became leader while node {acker} was rejoining (term {})",
                m.current_term
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // The old leader comes back; a leader is elected among the nodes with
    // data, and the acker catches up through an index learned from it.
    let (log, sm) = open(&f.dirs[&leader], leader);
    f.handles.insert(leader, sm.handle());
    f.cluster.start_node(leader, log, sm).await.unwrap();
    let idx = write(&f.cluster, &all, Op::Tick).await;
    f.cluster
        .wait_applied(&all, idx, Duration::from_secs(20))
        .await
        .unwrap();
    for id in all {
        assert!(
            f.handles[&id].conn_ids().contains(&x),
            "node {id} lost the committed Connect of {x}"
        );
    }

    // Leave rejoin mode; the cluster keeps working.
    gate.open();
    f.cluster.raft(acker).unwrap().runtime_config().elect(true);
    let idx = write(&f.cluster, &all, Op::Tick).await;
    f.cluster
        .wait_applied(&all, idx, Duration::from_secs(20))
        .await
        .unwrap();
}

/// The server's safe rejoin, on the simulated network: ask the other nodes
/// for their status until a majority of the cluster among them answered,
/// persist the highest vote with the log store's `save_vote`, then start
/// Raft in rejoin mode (elections off, vote gate closed).
async fn rejoin(
    f: &mut Fixture,
    id: NodeId,
    all: &[NodeId],
) -> (Arc<VoteGate>, Option<openraft::Vote<NodeId>>) {
    use openraft::storage::RaftLogStorage;
    let (mut log, sm) = open(&f.dirs[&id], id);
    f.net.set_status_source(id, Some(Arc::new(log.clone())));
    let peers: BTreeSet<NodeId> = all.iter().copied().collect();
    let node = f.net.node(id);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let vote = loop {
        let answers = status::probe(&node, id, &peers).await;
        if let Adopt::Vote(v) = status::adopt_vote(all.len(), &answers, None) {
            break v;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no quorum of status answers"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    if let Some(v) = &vote {
        log.save_vote(v).await.unwrap();
    }
    let gate = Arc::new(VoteGate::new(false));
    f.net.set_vote_gate(id, Some(gate.clone()));
    f.handles.insert(id, sm.handle());
    f.cluster.start_node(id, log.clone(), sm).await.unwrap();
    f.net.set_status_source(id, Some(Arc::new(log)));
    f.cluster.raft(id).unwrap().runtime_config().elect(false);
    (gate, vote)
}

/// Finding 6 (fixed by safe rejoin): rejoin mode kept a wiped node from
/// voting, but not from acknowledging entries. A leader of an old term
/// that never learned of the newer term (here the first leader, cut off
/// while the others elected a new leader and committed X) could still
/// replicate to the rejoining node, which had forgotten every term, and
/// commit with it as a majority: entries committed in the newer term were
/// overwritten. Now the rejoining node adopts the highest vote of a
/// majority of the other nodes before it starts Raft, so it rejects the
/// old leader (whose vote is lower), and the old leader steps down.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_leader_commits_through_a_rejoining_node() {
    let mut f = start3().await;
    let all = [1, 2, 3];
    for _ in 0..5 {
        write(&f.cluster, &all, Op::Tick).await;
    }
    let old = f
        .cluster
        .wait_for_leader(&all, Duration::from_secs(5))
        .await
        .unwrap();
    let old_term = f.cluster.raft(old).unwrap().metrics().borrow().current_term;
    let rest: Vec<NodeId> = all.into_iter().filter(|&i| i != old).collect();
    f.net.isolate(old, &all);
    let x = conn_id(1, 777);
    let idx = write(
        &f.cluster,
        &rest,
        Op::Conn {
            seq: 1,
            input: EngineInput::Connect(x),
        },
    )
    .await;
    let new = f
        .cluster
        .wait_for_leader(&rest, Duration::from_secs(5))
        .await
        .unwrap();
    let acker = rest.iter().copied().find(|&i| i != new).unwrap();
    f.cluster
        .wait_applied(&[acker], idx, Duration::from_secs(5))
        .await
        .unwrap();
    eprintln!("X committed at {idx} by leader {new} with {acker}; old leader {old} cut off");
    let m = f.cluster.raft(old).unwrap().metrics().borrow().clone();
    assert!(
        m.state.is_leader() && m.current_term == old_term,
        "the old leader must still believe it leads: {:?} term {}",
        m.state,
        m.current_term
    );

    // The acker is wiped and rejoins: it can reach the old leader and the
    // new one (the old leader still cannot reach the new one), adopts the
    // highest vote, starts in rejoin mode; then the new leader is cut off.
    f.cluster.stop_node(acker).await;
    wipe(&f.dirs[&acker]);
    f.net.unblock(old, acker);
    f.net.unblock(acker, old);
    let (_gate, vote) = rejoin(&mut f, acker, &all).await;
    eprintln!("node {acker} adopted {vote:?}");
    let adopted = vote.unwrap();
    assert!(adopted.leader_id().term > old_term, "{adopted}");
    f.net.isolate(new, &all);

    let r = tokio::time::timeout(
        Duration::from_secs(3),
        f.cluster.raft(old).unwrap().client_write(Request {
            now: 1,
            op: Op::Tick,
        }),
    )
    .await;
    if let Ok(Ok(resp)) = &r {
        f.cluster
            .wait_applied(&[acker], resp.log_id.index, Duration::from_secs(5))
            .await
            .unwrap();
    }
    assert!(
        !matches!(r, Ok(Ok(_))) || f.handles[&acker].conn_ids().contains(&x),
        "old leader {old} committed {r:?} through rejoining node {acker}, which lacks X \
         (committed in the newer term by {new})"
    );
    // The acker told the old leader about the newer vote: it no longer
    // leads in its old term.
    let stepped_down = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let m = f.cluster.raft(old).unwrap().metrics().borrow().clone();
            if !(m.state.is_leader() && m.current_term == old_term) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(
        stepped_down.is_ok(),
        "old leader {old} still leads in term {old_term}"
    );

    // Everyone reconnects: the cluster converges with X everywhere.
    f.net.heal();
    let idx = write(&f.cluster, &all, Op::Tick).await;
    f.cluster
        .wait_applied(&all, idx, Duration::from_secs(20))
        .await
        .unwrap();
    for id in all {
        assert!(
            f.handles[&id].conn_ids().contains(&x),
            "node {id} lacks the committed Connect of {x}"
        );
    }
}

/// Finding 3 (multi-process): see `bstk_chaos::mp::reconnect_storm_probe`.
/// Prints what it observed; fails if the isolated node queued more than a
/// few items per second of isolation. Fixed in P3-FB: an isolated node
/// refuses new connections at accept and queues nothing for them.
#[test]
fn reconnect_storm_to_an_isolated_node() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let r = rt.block_on(bstk_chaos::mp::reconnect_storm_probe(Duration::from_secs(
        5,
    )));
    rt.shutdown_background();
    let r = r.unwrap();
    eprintln!("{r}");
    let queued: u64 = r
        .split("queue held Some(")
        .nth(1)
        .and_then(|s| s.split(')').next())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    assert!(queued < 100, "{r}");
}

fn blank(term: u64, index: u64) -> openraft::Entry<bstk_raft::TypeConfig> {
    openraft::Entry {
        log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(term, 1), index),
        payload: openraft::EntryPayload::Blank,
    }
}

/// Finding 4 (fixed with a one-line change in `log_store.rs`): a batch
/// append started a new segment file for every entry after the first
/// (the rollover check compared the index with the segment's flushed
/// records only, not the ones pending in the batch). Followers, which
/// receive batches, ended up with one file (and one open descriptor, two
/// fsyncs and a directory fsync) per entry.
#[tokio::test]
async fn batch_append_stays_in_one_segment() {
    use openraft::storage::{RaftLogStorage, RaftLogStorageExt};
    let tmp = tempfile::tempdir().unwrap();
    let (mut log, _sm) = open(tmp.path(), 1);
    log.blocking_append((1..=10).map(|i| blank(1, i)))
        .await
        .unwrap();
    assert_eq!(log.metrics().segments, 1, "{:?}", log.metrics());
    log.truncate(blank(1, 8).log_id).await.unwrap();
    log.blocking_append((8..=12).map(|i| blank(2, i)))
        .await
        .unwrap();
    assert_eq!(log.metrics().segments, 1, "{:?}", log.metrics());
    assert_eq!(log.metrics().last_index, Some(12));
}
