//! Cluster mode (P3-T4): real `beanstalkd-rs` processes on 127.0.0.1
//! forming Raft clusters. Plaintext cluster traffic
//! (`insecure_plaintext = true`) except for the mTLS test.
//!
//! Every test finds the leader through `/admin` and never assumes which
//! node wins an election. Tests run one cluster at a time (`SERIAL`):
//! several clusters electing leaders at once on a loaded machine would
//! make the timing assertions (a new leader within 2 s) meaningless.

#![allow(clippy::unwrap_used)]

mod common;

use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{LazyLock, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use nix::sys::signal::Signal;
use serde_json::Value;

use common::p2::{BIN, Proto, claim_port, http, release, run, stat_of};

static SERIAL: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

/// Polls `f` every 20 ms until it returns `Some`, for at most `timeout`.
fn wait_for<T>(timeout: Duration, mut f: impl FnMut() -> Option<T>) -> Option<T> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(v) = f() {
            return Some(v);
        }
        if Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

type Client = Proto<TcpStream>;

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Opts {
    node_timeout: &'static str,
    snapshot_every: u64,
    /// Extra `[cluster]` lines (e.g. `[cluster.tls]`); `insecure_plaintext`
    /// is set unless this contains `[cluster.tls]`.
    extra: String,
}

impl Default for Opts {
    fn default() -> Self {
        Opts {
            node_timeout: "5s",
            snapshot_every: 100_000,
            extra: String::new(),
        }
    }
}

struct Node {
    id: u64,
    client: u16,
    http: u16,
    cluster: u16,
    data_dir: PathBuf,
    config: PathBuf,
    log: PathBuf,
    child: Option<Child>,
    starts: u32,
}

impl Node {
    fn client_addr(&self) -> SocketAddr {
        ([127, 0, 0, 1], self.client).into()
    }

    fn http_addr(&self) -> SocketAddr {
        ([127, 0, 0, 1], self.http).into()
    }

    fn start(&mut self, args: &[&str]) {
        assert!(self.child.is_none(), "node {} already running", self.id);
        self.starts += 1;
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log)
            .unwrap();
        let child = Command::new(BIN)
            .arg("--config")
            .arg(&self.config)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .unwrap();
        self.child = Some(child);
    }

    fn pid(&self) -> nix::unistd::Pid {
        let c = self.child.as_ref().expect("running");
        nix::unistd::Pid::from_raw(i32::try_from(c.id()).unwrap())
    }

    fn signal(&self, sig: Signal) {
        nix::sys::signal::kill(self.pid(), sig).unwrap();
    }

    fn kill9(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }

    /// Sends `sig` and waits for the exit status.
    fn stop(&mut self, sig: Signal) -> ExitStatus {
        self.signal(sig);
        let mut c = self.child.take().unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Some(st) = c.try_wait().unwrap() {
                return st;
            }
            assert!(Instant::now() < deadline, "node {} did not exit", self.id);
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn running(&mut self) -> bool {
        match &mut self.child {
            None => false,
            Some(c) => c.try_wait().unwrap().is_none(),
        }
    }

    /// `/readyz` status, 0 if the HTTP listener does not answer.
    fn readyz(&self) -> u16 {
        http(self.http_addr(), "GET", "/readyz", "").map_or(0, |r| r.status)
    }

    fn wait_ready(&self, timeout: Duration) -> bool {
        wait_for(timeout, || (self.readyz() == 200).then_some(())).is_some()
    }

    /// The `/admin` document, if served.
    fn admin(&self) -> Option<Value> {
        let r = http(self.http_addr(), "GET", "/admin", "").ok()?;
        if r.status != 200 {
            return None;
        }
        serde_json::from_str(&r.body).ok()
    }

    fn metrics(&self) -> String {
        let r = http(self.http_addr(), "GET", "/metrics", "").unwrap();
        assert_eq!(r.status, 200, "{}", r.body);
        r.body
    }

    fn connect(&self) -> Client {
        let s = TcpStream::connect_timeout(&self.client_addr(), Duration::from_secs(2))
            .unwrap_or_else(|e| panic!("connect to node {}: {e}", self.id));
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        Proto::new(s)
    }

    fn log_text(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// Removes the Raft state (the node must be stopped).
    fn wipe(&self) {
        assert!(self.child.is_none());
        std::fs::remove_dir_all(&self.data_dir).unwrap();
    }
}

struct Cluster {
    _dir: tempfile::TempDir,
    nodes: Vec<Node>,
    _serial: MutexGuard<'static, ()>,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        let panicking = std::thread::panicking();
        for n in &mut self.nodes {
            n.kill9();
            if panicking {
                let log = n.log_text();
                let tail: Vec<&str> = log.lines().rev().take(60).collect();
                eprintln!("--- node {} stderr (last lines) ---", n.id);
                for l in tail.iter().rev() {
                    eprintln!("{l}");
                }
            }
            for p in [n.client, n.http, n.cluster] {
                release(p);
            }
        }
    }
}

impl Cluster {
    /// Writes the configuration of `n` nodes (ids 1..=n) without starting
    /// them.
    fn configure(n: u64, opts: &Opts) -> Cluster {
        let guard = serial();
        let dir = tempfile::tempdir().unwrap();
        let ports: Vec<(u16, u16, u16)> = (0..n)
            .map(|_| (claim_port(), claim_port(), claim_port()))
            .collect();
        let peers: String = ports
            .iter()
            .enumerate()
            .map(|(i, p)| {
                format!(
                    "[[cluster.peer]]\nid = {}\naddr = \"127.0.0.1:{}\"\n",
                    i + 1,
                    p.2
                )
            })
            .collect();
        let security = if opts.extra.contains("[cluster.tls]") {
            ""
        } else {
            "insecure_plaintext = true\n"
        };
        let mut nodes = Vec::new();
        for (i, &(client, http, cluster)) in ports.iter().enumerate() {
            let id = i as u64 + 1;
            let data_dir = dir.path().join(format!("data{id}"));
            let config = dir.path().join(format!("node{id}.toml"));
            let text = format!(
                "[[listener]]\naddr = \"127.0.0.1:{client}\"\n\
                 [http]\naddr = \"127.0.0.1:{http}\"\nsnapshot_min_interval = \"0s\"\n\
                 [cluster]\nnode_id = {id}\nlisten = \"127.0.0.1:{cluster}\"\n\
                 data_dir = \"{}\"\nnode_timeout = \"{}\"\nsnapshot_every = {}\n{security}{}\n{peers}",
                data_dir.display(),
                opts.node_timeout,
                opts.snapshot_every,
                opts.extra.replace("{id}", &id.to_string()),
            );
            std::fs::write(&config, text).unwrap();
            nodes.push(Node {
                id,
                client,
                http,
                cluster,
                data_dir,
                config,
                log: dir.path().join(format!("node{id}.log")),
                child: None,
                starts: 0,
            });
        }
        Cluster {
            _dir: dir,
            nodes,
            _serial: guard,
        }
    }

    /// Starts `n` nodes, bootstraps from node 1 and waits until all are
    /// ready.
    fn start(n: u64, opts: &Opts) -> Cluster {
        let mut c = Cluster::configure(n, opts);
        c.nodes[0].start(&["--cluster-init"]);
        for node in &mut c.nodes[1..] {
            node.start(&[]);
        }
        c.wait_all_ready();
        c
    }

    fn wait_all_ready(&self) {
        for n in &self.nodes {
            if n.child.is_some() {
                assert!(
                    n.wait_ready(Duration::from_secs(20)),
                    "node {} never became ready",
                    n.id
                );
            }
        }
    }

    /// Index of the leader among the running nodes, per their `/admin`.
    fn leader(&mut self) -> usize {
        self.try_leader(Duration::from_secs(15))
            .expect("no leader elected")
    }

    fn try_leader(&mut self, timeout: Duration) -> Option<usize> {
        let running: Vec<usize> = (0..self.nodes.len())
            .filter(|&i| self.nodes[i].running())
            .collect();
        wait_for(timeout, || {
            running.iter().copied().find(|&i| {
                self.nodes[i].admin().is_some_and(|a| {
                    a["cluster"]["role"] == "leader" && a["cluster"]["ready"] == true
                })
            })
        })
    }

    /// Indexes of the running nodes other than `leader`.
    fn followers(&mut self, leader: usize) -> Vec<usize> {
        (0..self.nodes.len())
            .filter(|&i| i != leader && self.nodes[i].running())
            .collect()
    }
}

/// `RESERVED <id> <n>` + body: the job id.
fn reserve(c: &mut Client, line: &str) -> u64 {
    let (header, _) = c.body_reply(line);
    assert!(header.starts_with("RESERVED "), "{line}: {header}");
    header.split(' ').nth(1).unwrap().parse().unwrap()
}

fn read_reserved(c: &mut Client) -> (u64, Vec<u8>) {
    let header = c.read_line();
    assert!(header.starts_with("RESERVED "), "{header}");
    let mut parts = header.trim_end().split(' ');
    let id = parts.nth(1).unwrap().parse().unwrap();
    let n: usize = parts.next().unwrap().parse().unwrap();
    let mut body = Vec::new();
    while body.len() < n + 2 {
        body.extend(c.read_line().into_bytes());
    }
    body.truncate(n);
    (id, body)
}

fn inserted(reply: &str) -> u64 {
    reply
        .strip_prefix("INSERTED ")
        .unwrap_or_else(|| panic!("expected INSERTED, got {reply:?}"))
        .parse()
        .unwrap()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn bootstrap_and_operations_through_leader_and_follower() {
    let mut c = Cluster::start(3, &Opts::default());
    let l = c.leader();
    let fs = c.followers(l);
    assert_eq!(fs.len(), 2);
    // Every node agrees on the leader.
    let leader_id = c.nodes[l].id;
    for n in &c.nodes {
        let a = n.admin().unwrap();
        assert_eq!(a["cluster"]["leader_id"], leader_id, "{a}");
        assert_eq!(a["cluster"]["node_id"], n.id);
    }

    let mut on_leader = c.nodes[l].connect();
    let mut on_follower = c.nodes[fs[0]].connect();

    // Put on the leader, reserve and delete on a follower.
    let a = inserted(&on_leader.put(b"through-leader"));
    let got = reserve(&mut on_follower, "reserve-with-timeout 5");
    assert_eq!(got, a);
    assert_eq!(on_follower.cmd(&format!("delete {a}")), "DELETED");
    // Put on a follower, reserve and delete on the leader.
    let b = inserted(&on_follower.put(b"through-follower"));
    assert!(b > a);
    let (hdr, body) = on_leader.body_reply("reserve-with-timeout 5");
    assert_eq!(hdr, format!("RESERVED {b} 16"));
    assert_eq!(body, b"through-follower");
    assert_eq!(on_leader.cmd(&format!("delete {b}")), "DELETED");
    assert_eq!(on_leader.cmd(&format!("peek {b}")), "NOT_FOUND");

    // A waiting reserve on a follower is woken by a put on another node.
    let mut waiter = c.nodes[fs[1]].connect();
    waiter.send(b"reserve\r\n");
    let mut other = c.nodes[fs[0]].connect();
    // The waiter is registered once its reserve is applied.
    wait_for(Duration::from_secs(5), || {
        (other.stat("stats", "current-waiting") == "1").then_some(())
    })
    .expect("reserve never started waiting");
    let d = inserted(&other.put(b"wake"));
    let (id, body) = read_reserved(&mut waiter);
    assert_eq!((id, body.as_slice()), (d, &b"wake"[..]));
    assert_eq!(waiter.cmd(&format!("release {d} 0 0")), "RELEASED");

    // `stats` counters agree across nodes (identity fields differ).
    let keys = [
        "cmd-put",
        "cmd-delete",
        "cmd-reserve",
        "cmd-reserve-with-timeout",
        "cmd-release",
        "total-jobs",
        "current-jobs-ready",
        "current-connections",
        "total-connections",
        "current-tubes",
    ];
    let mut clients: Vec<Client> = c.nodes.iter().map(Node::connect).collect();
    // A reply means the connection's `Connect` is committed; every later
    // entry (the `stats` below) is applied after it on every node.
    for cl in &mut clients {
        assert_eq!(cl.cmd("list-tube-used"), "USING default");
    }
    let mut seen: Option<Vec<String>> = None;
    let mut pids = Vec::new();
    for cl in &mut clients {
        let y = cl.yaml("stats");
        pids.push(stat_of(&y, "pid"));
        let vals: Vec<String> = keys.iter().map(|k| stat_of(&y, k)).collect();
        match &seen {
            None => seen = Some(vals),
            // Earlier `stats` calls only change cmd-stats, which is not
            // compared.
            Some(first) => assert_eq!(first, &vals),
        }
    }
    let vals = seen.unwrap();
    assert_eq!(vals[0], "3", "cmd-put");
    assert_eq!(vals[5], "3", "total-jobs");
    assert_eq!(vals[6], "1", "current-jobs-ready");
    // `stats` shows the pid of the node the client is connected to.
    pids.sort();
    pids.dedup();
    assert_eq!(pids.len(), 3);

    // Cluster metrics.
    let m = c.nodes[l].metrics();
    assert!(
        m.contains("beanstalkd_cluster_role{role=\"leader\"} 1"),
        "{m}"
    );
    assert!(m.contains("beanstalkd_cluster_ready 1"), "{m}");
    for f in &fs {
        assert!(
            m.contains(&format!(
                "beanstalkd_cluster_replication_lag{{peer=\"{}\"}}",
                c.nodes[*f].id
            )),
            "{m}"
        );
    }
    let m = c.nodes[fs[0]].metrics();
    assert!(
        m.contains("beanstalkd_cluster_role{role=\"follower\"} 1"),
        "{m}"
    );
    assert!(
        m.contains(&format!("beanstalkd_cluster_leader_id {leader_id}")),
        "{m}"
    );
    assert!(m.contains("beanstalkd_cluster_forward_queue 0"), "{m}");
}

#[test]
fn leader_kill_keeps_follower_connections_and_reservations() {
    let mut c = Cluster::start(3, &Opts::default());
    let l = c.leader();
    let fs = c.followers(l);
    let (f, g) = (fs[0], fs[1]);

    let mut producer = c.nodes[g].connect();
    let job = inserted(&producer.put(b"held"));
    let mut worker = c.nodes[f].connect();
    assert_eq!(reserve(&mut worker, "reserve-with-timeout 5"), job);
    let mut waiter = c.nodes[f].connect();
    waiter.send(b"reserve\r\n");
    let mut probe = c.nodes[f].connect();
    wait_for(Duration::from_secs(5), || {
        (probe.stat("stats", "current-waiting") == "1").then_some(())
    })
    .expect("reserve never started waiting");

    let killed = Instant::now();
    c.nodes[l].kill9();

    // A new leader serves within 2 s: a put through a survivor commits.
    let reply = producer.put(b"after failover");
    let took = killed.elapsed();
    let second = inserted(&reply);
    assert!(
        took <= Duration::from_secs(2),
        "the put after the leader kill took {took:?}"
    );
    // The waiting reserve on the follower got it.
    let (id, body) = read_reserved(&mut waiter);
    assert_eq!((id, body.as_slice()), (second, &b"after failover"[..]));
    // The reservation survived: touch and delete still work.
    assert_eq!(worker.cmd(&format!("touch {job}")), "TOUCHED");
    assert_eq!(worker.cmd(&format!("delete {job}")), "DELETED");
    assert_eq!(waiter.cmd(&format!("delete {second}")), "DELETED");
    let nl = c.leader();
    assert_ne!(nl, l);
}

#[test]
fn follower_kill_releases_its_reservations_after_drop_node() {
    let opts = Opts {
        node_timeout: "1s",
        ..Opts::default()
    };
    let mut c = Cluster::start(3, &opts);
    let l = c.leader();
    let f = c.followers(l)[0];

    let mut on_leader = c.nodes[l].connect();
    let job = inserted(&on_leader.put(b"x"));
    let mut worker = c.nodes[f].connect();
    assert_eq!(reserve(&mut worker, "reserve-with-timeout 5"), job);
    let mut waiter = c.nodes[l].connect();
    waiter.send(b"reserve-with-timeout 30\r\n");

    let killed = Instant::now();
    c.nodes[f].kill9();
    // After 2 x node_timeout the leader drops the node: the job returns to
    // ready and goes to the waiting reserve.
    let (id, _) = read_reserved(&mut waiter);
    assert_eq!(id, job);
    let took = killed.elapsed();
    assert!(took >= Duration::from_millis(1900), "{took:?}");
    assert!(took < Duration::from_secs(10), "{took:?}");
    let a = c.nodes[l].admin().unwrap();
    assert!(
        a["cluster"]["drop_node_proposals"].as_u64().unwrap() >= 1,
        "{a}"
    );
    // Only the connections of the lost node were closed.
    assert_eq!(on_leader.stat("stats", "current-connections"), "2");
}

#[test]
fn restarted_node_drops_its_stale_connections() {
    // A long node_timeout: the leader does not drop the node by itself.
    let mut c = Cluster::start(3, &Opts::default());
    let l = c.leader();
    let f = c.followers(l)[0];

    let mut on_leader = c.nodes[l].connect();
    let job = inserted(&on_leader.put(b"x"));
    let mut worker = c.nodes[f].connect();
    assert_eq!(reserve(&mut worker, "reserve-with-timeout 5"), job);
    let mut idle = c.nodes[f].connect();
    assert_eq!(idle.cmd("use other"), "USING other");
    assert_eq!(on_leader.stat("stats", "current-connections"), "3");

    c.nodes[f].kill9();
    assert_eq!(
        on_leader.stat(&format!("stats-job {job}"), "state"),
        "reserved"
    );
    c.nodes[f].start(&[]);
    assert!(c.nodes[f].wait_ready(Duration::from_secs(20)));
    // `DropNode(self)` at startup closed out the old connections.
    assert_eq!(
        on_leader.stat(&format!("stats-job {job}"), "state"),
        "ready"
    );
    assert_eq!(on_leader.stat("stats", "current-connections"), "1");
    assert_eq!(on_leader.stat("stats", "current-tubes"), "1");

    // New connections work, and are numbered above the old ones.
    let mut fresh = c.nodes[f].connect();
    assert_eq!(reserve(&mut fresh, "reserve-with-timeout 5"), job);
    let second = inserted(&fresh.put(b"y"));
    assert_eq!(fresh.cmd(&format!("delete {job}")), "DELETED");
    assert_eq!(
        on_leader.stat(&format!("stats-job {second}"), "state"),
        "ready"
    );
    assert_eq!(c.nodes[f].starts, 2);
}

#[test]
fn wiped_node_rejoins_from_a_snapshot() {
    let opts = Opts {
        snapshot_every: 10,
        ..Opts::default()
    };
    let mut c = Cluster::start(3, &opts);
    let l = c.leader();
    let f = c.followers(l)[0];
    let mut on_leader = c.nodes[l].connect();
    let mut ids = Vec::new();
    for i in 0..60 {
        ids.push(inserted(&on_leader.put(format!("job {i}").as_bytes())));
    }

    c.nodes[f].kill9();
    c.nodes[f].wipe();
    // More entries, so the leader's log no longer reaches back far enough.
    for i in 60..80 {
        ids.push(inserted(&on_leader.put(format!("job {i}").as_bytes())));
    }
    wait_for(Duration::from_secs(10), || {
        let a = c.nodes[l].admin()?;
        let snap = a["cluster"]["snapshot_index"].as_u64()?;
        (snap > 60 && a["cluster"]["log_first_index"].as_u64()? > 10).then_some(())
    })
    .expect("the leader never snapshotted and purged its log");

    c.nodes[f].start(&[]);
    assert!(
        c.nodes[f].wait_ready(Duration::from_secs(20)),
        "wiped node never became ready"
    );
    let a = c.nodes[f].admin().unwrap();
    assert!(a["cluster"]["snapshot_index"].as_u64().is_some(), "{a}");
    let mut on_f = c.nodes[f].connect();
    assert_eq!(on_f.stat("stats", "current-jobs-ready"), "80");
    assert_eq!(on_f.stat("stats", "total-jobs"), "80");
    let (hdr, body) = on_f.body_reply(&format!("peek {}", ids[3]));
    assert_eq!(hdr, format!("FOUND {} 5", ids[3]));
    assert_eq!(body, b"job 3");
    assert_eq!(reserve(&mut on_f, "reserve-with-timeout 5"), ids[0]);
}

#[test]
fn full_cluster_kill_and_restart_loses_no_committed_job() {
    let mut c = Cluster::start(3, &Opts::default());
    let l = c.leader();
    let fs = c.followers(l);
    let mut clients = [
        c.nodes[l].connect(),
        c.nodes[fs[0]].connect(),
        c.nodes[fs[1]].connect(),
    ];
    let mut ids = Vec::new();
    for i in 0..30 {
        let cl = &mut clients[i % 3];
        ids.push(inserted(&cl.put(format!("body {i}").as_bytes())));
    }
    for &id in &ids[..5] {
        assert_eq!(clients[1].cmd(&format!("delete {id}")), "DELETED");
    }
    // A reservation held when everything dies returns to ready.
    assert_eq!(reserve(&mut clients[2], "reserve-with-timeout 5"), ids[5]);
    drop(clients);

    for n in &mut c.nodes {
        n.kill9();
    }
    for n in &mut c.nodes {
        n.start(&[]);
    }
    c.wait_all_ready();
    let l = c.leader();
    let mut cl = c.nodes[l].connect();
    assert_eq!(cl.stat("stats", "current-jobs-ready"), "25");
    for &id in &ids[..5] {
        assert_eq!(cl.cmd(&format!("peek {id}")), "NOT_FOUND");
    }
    for (i, &id) in ids.iter().enumerate().skip(5) {
        let (hdr, body) = cl.body_reply(&format!("peek {id}"));
        assert!(hdr.starts_with(&format!("FOUND {id} ")), "{hdr}");
        assert_eq!(body, format!("body {i}").as_bytes());
    }
    // New ids continue after the old ones.
    assert!(inserted(&cl.put(b"new")) > *ids.last().unwrap());
}

#[test]
fn sigusr1_on_a_follower_drains_the_cluster() {
    let mut c = Cluster::start(3, &Opts::default());
    let l = c.leader();
    let f = c.followers(l)[0];
    let mut on_leader = c.nodes[l].connect();
    inserted(&on_leader.put(b"before"));
    c.nodes[f].signal(Signal::SIGUSR1);
    wait_for(Duration::from_secs(5), || {
        (on_leader.stat("stats", "draining") == "true").then_some(())
    })
    .expect("drain mode never reached the leader");
    assert_eq!(on_leader.put(b"after"), "DRAINING");
    for n in &c.nodes {
        let mut cl = n.connect();
        assert_eq!(cl.stat("stats", "draining"), "true", "node {}", n.id);
    }
}

#[test]
fn readyz_follows_leader_and_catch_up() {
    let mut c = Cluster::configure(3, &Opts::default());
    // Nodes without state wait to be contacted: not ready.
    c.nodes[1].start(&[]);
    c.nodes[2].start(&[]);
    wait_for(Duration::from_secs(10), || {
        (c.nodes[1].readyz() == 503 && c.nodes[2].readyz() == 503).then_some(())
    })
    .expect("HTTP never answered");
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(c.nodes[1].readyz(), 503);
    // Bootstrapping makes every node ready.
    c.nodes[0].start(&["--cluster-init"]);
    c.wait_all_ready();
    let l = c.leader();
    let fs = c.followers(l);
    // A lone survivor has no leader: not ready.
    c.nodes[l].kill9();
    c.nodes[fs[0]].kill9();
    assert!(
        wait_for(Duration::from_secs(5), || (c.nodes[fs[1]].readyz() == 503)
            .then_some(()))
        .is_some(),
        "a node without a leader stayed ready"
    );
    let a = c.nodes[fs[1]].admin().unwrap();
    assert_eq!(a["cluster"]["ready"], false, "{a}");
    // A majority again: ready.
    c.nodes[fs[0]].start(&[]);
    assert!(c.nodes[fs[1]].wait_ready(Duration::from_secs(20)));
    assert!(c.nodes[fs[0]].wait_ready(Duration::from_secs(20)));
}

#[test]
fn mismatched_max_job_size_is_rejected() {
    let mut c = Cluster::configure(3, &Opts::default());
    c.nodes[0].start(&["--cluster-init"]);
    c.nodes[1].start(&[]);
    c.nodes[2].start(&["-z", "1000"]);
    assert!(c.nodes[0].wait_ready(Duration::from_secs(20)));
    assert!(c.nodes[1].wait_ready(Duration::from_secs(20)));
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(c.nodes[2].readyz(), 503);
    let found = wait_for(Duration::from_secs(5), || {
        let logs = format!("{}{}", c.nodes[0].log_text(), c.nodes[2].log_text());
        logs.contains("max_job_size mismatch").then_some(())
    });
    assert!(found.is_some(), "no mismatch error was logged");
    // The two matching nodes serve.
    let mut cl = c.nodes[1].connect();
    inserted(&cl.put(b"ok"));
}

#[test]
fn graceful_shutdown_disconnects_clients() {
    let mut c = Cluster::start(3, &Opts::default());
    let l = c.leader();
    let f = c.followers(l)[0];
    let mut on_leader = c.nodes[l].connect();
    let job = inserted(&on_leader.put(b"x"));
    let mut worker = c.nodes[f].connect();
    assert_eq!(reserve(&mut worker, "reserve-with-timeout 5"), job);
    let status = c.nodes[f].stop(Signal::SIGTERM);
    assert_eq!(status.code(), Some(0), "{}", c.nodes[f].log_text());
    // Its Disconnect was proposed before it left: the job is ready at once.
    assert_eq!(
        on_leader.stat(&format!("stats-job {job}"), "state"),
        "ready"
    );
    assert_eq!(on_leader.stat("stats", "current-connections"), "1");
}

// ---------------------------------------------------------------------------
// mTLS
// ---------------------------------------------------------------------------

fn write_cluster_pki(dir: &Path, n: u64) {
    use rcgen::{
        BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
        KeyUsagePurpose,
    };
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(DnType::CommonName, "bstk cluster test CA");
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let ca_key = KeyPair::generate().unwrap();
    let ca = params.self_signed(&ca_key).unwrap();
    std::fs::write(dir.join("cluster-ca.pem"), ca.pem()).unwrap();
    for id in 1..=n {
        let mut p = CertificateParams::new(vec![format!("bstk-node-{id}")]).unwrap();
        p.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
        ];
        p.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let key = KeyPair::generate().unwrap();
        let cert = p.signed_by(&key, &ca, &ca_key).unwrap();
        std::fs::write(dir.join(format!("node{id}.pem")), cert.pem()).unwrap();
        std::fs::write(dir.join(format!("node{id}.key")), key.serialize_pem()).unwrap();
    }
}

#[test]
fn mtls_cluster_replicates() {
    let pki = tempfile::tempdir().unwrap();
    write_cluster_pki(pki.path(), 3);
    let p = pki.path().display();
    let opts = Opts {
        extra: format!(
            "[cluster.tls]\ncert = \"{p}/node{{id}}.pem\"\nkey = \"{p}/node{{id}}.key\"\n\
             ca = \"{p}/cluster-ca.pem\"\n"
        ),
        ..Opts::default()
    };
    let mut c = Cluster::configure(3, &opts);
    let (status, out, err) = run(&[
        "--config",
        c.nodes[0].config.to_str().unwrap(),
        "--check-config",
    ]);
    assert_eq!(status.code(), Some(0), "{err}");
    assert!(out.contains("cluster tls: cert"), "{out}");
    c.nodes[0].start(&["--cluster-init"]);
    c.nodes[1].start(&[]);
    c.nodes[2].start(&[]);
    c.wait_all_ready();
    let l = c.leader();
    let f = c.followers(l)[0];
    let mut on_f = c.nodes[f].connect();
    let job = inserted(&on_f.put(b"secure"));
    let mut on_l = c.nodes[l].connect();
    assert_eq!(reserve(&mut on_l, "reserve-with-timeout 5"), job);
}

// ---------------------------------------------------------------------------
// Configuration errors
// ---------------------------------------------------------------------------

/// A one-node cluster configuration in `dir` (plus `extra` in
/// `[cluster]`, and `peers` instead of the default peer list).
fn single_node_config(dir: &Path, cluster_extra: &str, peers: Option<&str>) -> String {
    let (client, http, cluster) = (claim_port(), claim_port(), claim_port());
    let peers = peers.map_or_else(
        || format!("[[cluster.peer]]\nid = 1\naddr = \"127.0.0.1:{cluster}\"\n"),
        str::to_owned,
    );
    let text = format!(
        "[[listener]]\naddr = \"127.0.0.1:{client}\"\n[http]\naddr = \"127.0.0.1:{http}\"\n\
         [cluster]\nnode_id = 1\nlisten = \"127.0.0.1:{cluster}\"\ndata_dir = \"{}\"\n{cluster_extra}\n{peers}",
        dir.join("data").display()
    );
    let path = dir.join(format!("c{client}.toml"));
    std::fs::write(&path, text).unwrap();
    path.display().to_string()
}

fn check(path: &str, extra: &[&str]) -> (ExitStatus, String) {
    let mut args = vec!["--config", path, "--check-config"];
    args.extend_from_slice(extra);
    let (status, _, err) = run(&args);
    (status, err)
}

#[test]
fn cluster_configuration_errors() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    let plain = "insecure_plaintext = true";

    // A valid one-node configuration.
    let ok = single_node_config(dir.path(), plain, None);
    let (st, err) = check(&ok, &[]);
    assert_eq!(st.code(), Some(0), "{err}");

    let cases: Vec<(String, Vec<&str>, &str)> = vec![
        // -b with [cluster].
        (
            ok.clone(),
            vec!["-b", "/nonexistent/wal"],
            "cannot be used with [cluster]",
        ),
        // TLS required without insecure_plaintext.
        (
            single_node_config(dir.path(), "", None),
            vec![],
            "[cluster.tls]",
        ),
        // Incomplete [cluster.tls].
        (
            single_node_config(dir.path(), "[cluster.tls]\ncert = \"a.pem\"\n", None),
            vec![],
            "cluster.tls.key is required",
        ),
        // Two peers.
        (
            single_node_config(
                dir.path(),
                plain,
                Some(
                    "[[cluster.peer]]\nid = 1\naddr = \"127.0.0.1:1\"\n\
                     [[cluster.peer]]\nid = 2\naddr = \"127.0.0.1:2\"\n",
                ),
            ),
            vec![],
            "3 or 5 nodes",
        ),
        // Duplicate ids.
        (
            single_node_config(
                dir.path(),
                plain,
                Some(
                    "[[cluster.peer]]\nid = 1\naddr = \"127.0.0.1:1\"\n\
                     [[cluster.peer]]\nid = 1\naddr = \"127.0.0.1:2\"\n\
                     [[cluster.peer]]\nid = 3\naddr = \"127.0.0.1:3\"\n",
                ),
            ),
            vec![],
            "listed more than once",
        ),
        // Duplicate addresses.
        (
            single_node_config(
                dir.path(),
                plain,
                Some(
                    "[[cluster.peer]]\nid = 1\naddr = \"127.0.0.1:1\"\n\
                     [[cluster.peer]]\nid = 2\naddr = \"127.0.0.1:1\"\n\
                     [[cluster.peer]]\nid = 3\naddr = \"127.0.0.1:3\"\n",
                ),
            ),
            vec![],
            "used by another peer",
        ),
        // This node is not a peer.
        (
            single_node_config(
                dir.path(),
                plain,
                Some("[[cluster.peer]]\nid = 2\naddr = \"127.0.0.1:2\"\n"),
            ),
            vec![],
            "not listed in [[cluster.peer]]",
        ),
        // Timing.
        (
            single_node_config(dir.path(), &format!("{plain}\nheartbeat = \"200ms\""), None),
            vec![],
            "must be shorter than the minimum election timeout",
        ),
        (
            single_node_config(
                dir.path(),
                &format!("{plain}\nelection_timeout = [\"500ms\", \"300ms\"]"),
                None,
            ),
            vec![],
            "exceeds the maximum",
        ),
        (
            single_node_config(
                dir.path(),
                &format!("{plain}\nnode_timeout = \"soon\""),
                None,
            ),
            vec![],
            "cluster.node_timeout",
        ),
        // A job must fit in one cluster frame.
        (
            ok.clone(),
            vec!["-z", "100000000"],
            "too large for cluster mode",
        ),
        // Unknown keys.
        (
            single_node_config(dir.path(), &format!("{plain}\nbogus = 1"), None),
            vec![],
            "bogus",
        ),
    ];
    for (path, extra, expected) in cases {
        let (st, err) = check(&path, &extra);
        assert_eq!(st.code(), Some(1), "{extra:?} {expected}: {err}");
        assert!(err.contains(expected), "expected {expected:?} in {err}");
    }
    // --cluster-init without [cluster].
    let (st, _, err) = run(&["-l", "127.0.0.1", "-p", "0", "--cluster-init"]);
    assert_eq!(st.code(), Some(1), "{err}");
    assert!(err.contains("--cluster-init requires"), "{err}");
}

#[test]
fn cluster_init_refuses_existing_state_and_the_data_dir_is_locked() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    let cfg = single_node_config(dir.path(), "insecure_plaintext = true", None);
    let text = std::fs::read_to_string(&cfg).unwrap();
    let http_addr: SocketAddr = text
        .lines()
        .skip_while(|l| *l != "[http]")
        .nth(1)
        .unwrap()
        .trim_start_matches("addr = \"")
        .trim_end_matches('"')
        .parse()
        .unwrap();
    let mut first = Command::new(BIN)
        .args(["--config", &cfg, "--cluster-init"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let ready = wait_for(Duration::from_secs(20), || {
        http(http_addr, "GET", "/readyz", "")
            .ok()
            .filter(|r| r.status == 200)
    });
    assert!(ready.is_some(), "one-node cluster never became ready");

    // Another process on the same data directory: exit status 10.
    let other = single_node_config(dir.path(), "insecure_plaintext = true", None);
    let other_text = std::fs::read_to_string(&other).unwrap();
    // Same data_dir as `cfg` (both use <dir>/data).
    assert!(other_text.contains(&dir.path().join("data").display().to_string()));
    let (st, _, err) = run(&["--config", &other]);
    assert_eq!(st.code(), Some(10), "{err}");
    assert!(err.contains("lock"), "{err}");

    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(i32::try_from(first.id()).unwrap()),
        Signal::SIGTERM,
    )
    .unwrap();
    let st = first.wait().unwrap();
    assert_eq!(st.code(), Some(0));

    // The data directory now holds state: --cluster-init is refused.
    let (st, _, err) = run(&["--config", &cfg, "--cluster-init"]);
    assert_eq!(st.code(), Some(1), "{err}");
    assert!(err.contains("already holds Raft state"), "{err}");
}
