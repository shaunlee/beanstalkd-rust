//! Minimal reproductions of the problems the chaos runs found, on the
//! real storage over the simulated network (`SimCluster`). Those that
//! still fail are ignored; run them with
//! `cargo test -p bstk-chaos --test findings -- --ignored --nocapture`.

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use bstk_engine::{ConnId, EngineConfig, EngineInput, StaticSysInfo};
use bstk_proto::Response;
use bstk_raft::sim::{SimCluster, SimConfig, SimNetwork};
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

/// Finding 2: a committed entry is lost when a node that acknowledged it
/// rejoins with an empty data directory and then votes. The entry (a
/// `Connect` of connection X) is committed on the leader and one follower
/// while the third node is partitioned away; that follower is wiped and
/// restarted, the leader dies, the partition heals, and the two remaining
/// nodes elect the node that never had the entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "finding: fails until the lead decides on a fix"]
async fn wiped_voter_loses_a_committed_entry() {
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

    f.cluster.stop_node(acker).await;
    wipe(&f.dirs[&acker]);
    let (log, sm) = open(&f.dirs[&acker], acker);
    f.handles.insert(acker, sm.handle());
    f.cluster.start_node(acker, log, sm).await.unwrap();
    f.cluster.stop_node(leader).await;
    f.net.heal();

    let rest = [behind, acker];
    let idx = write(&f.cluster, &rest, Op::Tick).await;
    f.cluster
        .wait_applied(&rest, idx, Duration::from_secs(10))
        .await
        .unwrap();
    for id in rest {
        assert!(
            f.handles[&id].conn_ids().contains(&x),
            "node {id} lost the committed Connect of {x}"
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
