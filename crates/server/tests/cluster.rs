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

use std::collections::{BTreeMap, BTreeSet};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use nix::sys::signal::Signal;
use serde_json::Value;

use common::p2::{BIN, End, Proto, claim_port, http, release, run, stat_of};

static SERIAL: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

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

#[derive(Clone)]
struct Opts {
    node_timeout: &'static str,
    snapshot_every: u64,
    /// Extra `[cluster]` lines (e.g. `[cluster.tls]`); `insecure_plaintext`
    /// is set unless this contains `[cluster.tls]`.
    extra: String,
    /// Route every directed cluster link `a -> b` (connections node `a`
    /// dials to node `b`) through its own [`Proxy`].
    proxied: bool,
}

impl Default for Opts {
    fn default() -> Self {
        Opts {
            node_timeout: "5s",
            snapshot_every: 100_000,
            extra: String::new(),
            proxied: false,
        }
    }
}

/// A TCP proxy for one directed cluster link that can be cut: while cut,
/// its connections are closed and new ones are closed at accept, so
/// nothing the dialing node sends on this link (requests, and the answers
/// to them) gets through. The opposite direction is a separate link. While
/// muted, requests get through but the target's answers are discarded.
struct Proxy {
    addr: SocketAddr,
    cut: Arc<AtomicBool>,
    mute: Arc<AtomicBool>,
    conns: Arc<Mutex<Vec<TcpStream>>>,
}

impl Proxy {
    fn start(target: SocketAddr) -> Proxy {
        // On a claimed port: any free port could be a node's, claimed but
        // not yet bound. Never released, as the listener is never closed.
        let listener = loop {
            if let Ok(l) = TcpListener::bind(("127.0.0.1", claim_port())) {
                break l;
            }
        };
        let addr = listener.local_addr().unwrap();
        let cut = Arc::new(AtomicBool::new(false));
        let mute = Arc::new(AtomicBool::new(false));
        let conns: Arc<Mutex<Vec<TcpStream>>> = Arc::new(Mutex::new(Vec::new()));
        let (cut2, mute2, conns2) = (cut.clone(), mute.clone(), conns.clone());
        std::thread::spawn(move || {
            for down in listener.incoming() {
                let Ok(down) = down else { continue };
                if cut2.load(Ordering::SeqCst) {
                    continue;
                }
                let Ok(up) = TcpStream::connect(target) else {
                    continue;
                };
                let _ = down.set_nodelay(true);
                let _ = up.set_nodelay(true);
                {
                    let mut cs = conns2.lock().unwrap();
                    cs.push(down.try_clone().unwrap());
                    cs.push(up.try_clone().unwrap());
                }
                for (mut from, mut to, mute) in [
                    (down.try_clone().unwrap(), up.try_clone().unwrap(), None),
                    (up, down, Some(mute2.clone())),
                ] {
                    std::thread::spawn(move || {
                        use std::io::{Read, Write};
                        let mut buf = [0u8; 16 * 1024];
                        while let Ok(n @ 1..) = from.read(&mut buf) {
                            if mute.as_ref().is_some_and(|m| m.load(Ordering::SeqCst)) {
                                continue;
                            }
                            if to.write_all(&buf[..n]).is_err() {
                                break;
                            }
                        }
                        let _ = to.shutdown(Shutdown::Both);
                        let _ = from.shutdown(Shutdown::Both);
                    });
                }
            }
        });
        Proxy {
            addr,
            cut,
            mute,
            conns,
        }
    }

    fn cut(&self) {
        self.cut.store(true, Ordering::SeqCst);
        for c in self.conns.lock().unwrap().drain(..) {
            let _ = c.shutdown(Shutdown::Both);
        }
    }

    fn heal(&self) {
        self.cut.store(false, Ordering::SeqCst);
        self.mute.store(false, Ordering::SeqCst);
    }

    /// Lets requests through but discards the answers (see [`Proxy`]).
    #[cfg(feature = "test-hooks")]
    fn mute_replies(&self) {
        self.mute.store(true, Ordering::SeqCst);
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

    fn readyz(&self) -> u16 {
        http(self.http_addr(), "GET", "/readyz", "").map_or(0, |r| r.status)
    }

    fn wait_ready(&self, timeout: Duration) -> bool {
        wait_for(timeout, || (self.readyz() == 200).then_some(())).is_some()
    }

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

    fn wait_log(&self, text: &str, timeout: Duration) -> bool {
        wait_for(timeout, || self.log_text().contains(text).then_some(())).is_some()
    }

    /// Whether the data directory holds a Raft vote or log segment (the
    /// node initialized, or took part in a cluster).
    fn has_raft_state(&self) -> bool {
        std::fs::read_dir(self.data_dir.join("log")).is_ok_and(|rd| {
            rd.filter_map(Result::ok).any(|e| {
                let n = e.file_name().to_string_lossy().into_owned();
                n == "vote" || n.ends_with(".seg")
            })
        })
    }

    fn wipe(&self) {
        assert!(self.child.is_none());
        std::fs::remove_dir_all(&self.data_dir).unwrap();
    }
}

struct Cluster {
    _dir: tempfile::TempDir,
    nodes: Vec<Node>,
    proxies: BTreeMap<(u64, u64), Proxy>,
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
        let mut proxies = BTreeMap::new();
        if opts.proxied {
            for a in 1..=n {
                for (j, p) in ports.iter().enumerate() {
                    let b = j as u64 + 1;
                    if a != b {
                        proxies.insert((a, b), Proxy::start(([127, 0, 0, 1], p.2).into()));
                    }
                }
            }
        }
        let peers = |a: u64| -> String {
            ports
                .iter()
                .enumerate()
                .map(|(i, p)| {
                    let b = i as u64 + 1;
                    let addr = proxies
                        .get(&(a, b))
                        .map_or_else(|| format!("127.0.0.1:{}", p.2), |x| x.addr.to_string());
                    format!("[[cluster.peer]]\nid = {b}\naddr = \"{addr}\"\n")
                })
                .collect()
        };
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
                peers = peers(id),
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
            proxies,
            _serial: guard,
        }
    }

    /// Starts `n` nodes, bootstrapping every one with `--cluster-init`,
    /// and waits until all are ready.
    fn start(n: u64, opts: &Opts) -> Cluster {
        let mut c = Cluster::configure(n, opts);
        for node in &mut c.nodes {
            node.start(&["--cluster-init"]);
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

    fn cut_node(&self, id: u64) {
        for (&(a, b), p) in &self.proxies {
            if a == id || b == id {
                p.cut();
            }
        }
    }

    fn heal_pair(&self, a: u64, b: u64) {
        self.proxies[&(a, b)].heal();
        self.proxies[&(b, a)].heal();
    }

    fn heal_all(&self) {
        for p in self.proxies.values() {
            p.heal();
        }
    }

    fn followers(&mut self, leader: usize) -> Vec<usize> {
        (0..self.nodes.len())
            .filter(|&i| i != leader && self.nodes[i].running())
            .collect()
    }
}

impl Cluster {
    /// Configures node `n + 1` (not started, not a member) with `peers`
    /// as its `[[cluster.peer]]` list (ids of this cluster, plus `extra`
    /// unbound entries); the existing nodes' configs are left alone, so
    /// they know it only from the membership.
    fn configure_extra_node(&mut self, peers: &[u64], extra: &[u64], opts: &Opts) -> usize {
        let id = self.nodes.len() as u64 + 1;
        let (client, http, cluster) = (claim_port(), claim_port(), claim_port());
        let dir = self._dir.path().to_path_buf();
        let mut lines = String::new();
        for &p in peers {
            let port = if p == id {
                cluster
            } else {
                self.nodes[p as usize - 1].cluster
            };
            lines.push_str(&format!(
                "[[cluster.peer]]\nid = {p}\naddr = \"127.0.0.1:{port}\"\n"
            ));
        }
        for &p in extra {
            // A claimed port nobody listens on.
            lines.push_str(&format!(
                "[[cluster.peer]]\nid = {p}\naddr = \"127.0.0.1:{}\"\n",
                claim_port()
            ));
        }
        let node = Node {
            id,
            client,
            http,
            cluster,
            data_dir: dir.join(format!("data{id}")),
            config: dir.join(format!("node{id}.toml")),
            log: dir.join(format!("node{id}.log")),
            child: None,
            starts: 0,
        };
        write_node_config(&node, opts, &lines);
        self.nodes.push(node);
        self.nodes.len() - 1
    }
}

fn write_node_config(n: &Node, opts: &Opts, peers: &str) {
    let security = if opts.extra.contains("[cluster.tls]") {
        ""
    } else {
        "insecure_plaintext = true\n"
    };
    let text = format!(
        "[[listener]]\naddr = \"127.0.0.1:{}\"\n\
         [http]\naddr = \"127.0.0.1:{}\"\nsnapshot_min_interval = \"0s\"\n\
         [cluster]\nnode_id = {}\nlisten = \"127.0.0.1:{}\"\n\
         data_dir = \"{}\"\nnode_timeout = \"{}\"\nsnapshot_every = {}\n{security}{}\n{peers}",
        n.client,
        n.http,
        n.id,
        n.cluster,
        n.data_dir.display(),
        opts.node_timeout,
        opts.snapshot_every,
        opts.extra.replace("{id}", &n.id.to_string()),
    );
    std::fs::write(&n.config, text).unwrap();
}

#[cfg(feature = "test-hooks")]
impl Node {
    /// Waits until the process has exited; its exit status.
    fn wait_exit(&mut self, timeout: Duration) -> Option<ExitStatus> {
        let c = self.child.as_mut()?;
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(st) = c.try_wait().unwrap() {
                self.child = None;
                return Some(st);
            }
            if Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

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

#[test]
fn bootstrap_and_operations_through_leader_and_follower() {
    let mut c = Cluster::start(3, &Opts::default());
    let l = c.leader();
    let fs = c.followers(l);
    assert_eq!(fs.len(), 2);
    let leader_id = c.nodes[l].id;
    for n in &c.nodes {
        let a = n.admin().unwrap();
        assert_eq!(a["cluster"]["leader_id"], leader_id, "{a}");
        assert_eq!(a["cluster"]["node_id"], n.id);
    }

    let mut on_leader = c.nodes[l].connect();
    let mut on_follower = c.nodes[fs[0]].connect();

    let a = inserted(&on_leader.put(b"through-leader"));
    let got = reserve(&mut on_follower, "reserve-with-timeout 5");
    assert_eq!(got, a);
    assert_eq!(on_follower.cmd(&format!("delete {a}")), "DELETED");
    let b = inserted(&on_follower.put(b"through-follower"));
    assert!(b > a);
    let (hdr, body) = on_leader.body_reply("reserve-with-timeout 5");
    assert_eq!(hdr, format!("RESERVED {b} 16"));
    assert_eq!(body, b"through-follower");
    assert_eq!(on_leader.cmd(&format!("delete {b}")), "DELETED");
    assert_eq!(on_leader.cmd(&format!("peek {b}")), "NOT_FOUND");

    let mut waiter = c.nodes[fs[1]].connect();
    waiter.send(b"reserve\r\n");
    let mut other = c.nodes[fs[0]].connect();
    wait_for(Duration::from_secs(5), || {
        (other.stat("stats", "current-waiting") == "1").then_some(())
    })
    .expect("reserve never started waiting");
    let d = inserted(&other.put(b"wake"));
    let (id, body) = read_reserved(&mut waiter);
    assert_eq!((id, body.as_slice()), (d, &b"wake"[..]));
    assert_eq!(waiter.cmd(&format!("release {d} 0 0")), "RELEASED");

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
            Some(first) => assert_eq!(first, &vals),
        }
    }
    let vals = seen.unwrap();
    assert_eq!(vals[0], "3", "cmd-put");
    assert_eq!(vals[5], "3", "total-jobs");
    assert_eq!(vals[6], "1", "current-jobs-ready");
    pids.sort();
    pids.dedup();
    assert_eq!(pids.len(), 3);

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

    let reply = producer.put(b"after failover");
    let took = killed.elapsed();
    let second = inserted(&reply);
    assert!(
        took <= Duration::from_secs(2),
        "the put after the leader kill took {took:?}"
    );
    let (id, body) = read_reserved(&mut waiter);
    assert_eq!((id, body.as_slice()), (second, &b"after failover"[..]));
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
    // The leader last hears from the follower after this (the reserve, or a
    // later ping), so it may drop it only 2 x node_timeout after this, less
    // up to one heartbeat: the replication side counts too, and its last
    // answer may be the heartbeat before the reserve.
    let heard = Instant::now();
    assert_eq!(reserve(&mut worker, "reserve-with-timeout 5"), job);
    let mut waiter = c.nodes[l].connect();
    waiter.send(b"reserve-with-timeout 30\r\n");

    let killed = Instant::now();
    c.nodes[f].kill9();
    // After 2 x node_timeout the leader drops the node: the job returns to
    // ready and goes to the waiting reserve.
    let (id, _) = read_reserved(&mut waiter);
    assert_eq!(id, job);
    let silent = heard.elapsed();
    assert!(silent >= Duration::from_millis(1900), "{silent:?}");
    let took = killed.elapsed();
    assert!(took < Duration::from_secs(10), "{took:?}");
    let a = c.nodes[l].admin().unwrap();
    assert!(
        a["cluster"]["drop_node_proposals"].as_u64().unwrap() >= 1,
        "{a}"
    );
    assert_eq!(on_leader.stat("stats", "current-connections"), "2");
}

#[test]
fn restarted_node_drops_its_stale_connections() {
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
    assert_eq!(
        on_leader.stat(&format!("stats-job {job}"), "state"),
        "ready"
    );
    assert_eq!(on_leader.stat("stats", "current-connections"), "1");
    assert_eq!(on_leader.stat("stats", "current-tubes"), "1");

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
    c.nodes[1].start(&["--cluster-init"]);
    wait_for(Duration::from_secs(10), || {
        (c.nodes[1].readyz() == 503).then_some(())
    })
    .expect("HTTP never answered");
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(c.nodes[1].readyz(), 503);
    c.nodes[0].start(&["--cluster-init"]);
    c.nodes[2].start(&["--cluster-init"]);
    c.wait_all_ready();
    let l = c.leader();
    let fs = c.followers(l);
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
    c.nodes[fs[0]].start(&[]);
    assert!(c.nodes[fs[1]].wait_ready(Duration::from_secs(20)));
    assert!(c.nodes[fs[0]].wait_ready(Duration::from_secs(20)));
}

#[test]
fn mismatched_max_job_size_is_rejected() {
    // (Bootstrapping needs the status of a majority of the other nodes, so
    // for 3 nodes all of them: node 3 joins with the right -z first, then
    // restarts with another.)
    let mut c = Cluster::start(3, &Opts::default());
    c.nodes[2].kill9();
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
    assert_eq!(
        on_leader.stat(&format!("stats-job {job}"), "state"),
        "ready"
    );
    assert_eq!(on_leader.stat("stats", "current-connections"), "1");
}

/// Chaos finding 3: a node that lost its data and rejoined as a voter
/// could elect a leader lacking an entry it had acknowledged. Now it
/// rejoins without voting until it has caught up, and stays in rejoin mode
/// across a crash.
#[test]
fn wiped_node_does_not_vote_until_it_has_caught_up() {
    let mut c = Cluster::start(3, &Opts::default());
    let l = c.leader();
    let fs = c.followers(l);
    let (behind, acker) = (fs[0], fs[1]);
    let mut on_leader = c.nodes[l].connect();
    inserted(&on_leader.put(b"before"));

    // The bootstrap may already have sent `acker` through rejoin mode once
    // (docs/DESIGN.md §8 "Bootstrap"), so its log is counted from here.
    let count = |n: &Node, what: &str| n.log_text().matches(what).count();
    let rejoins = count(&c.nodes[acker], "rejoin mode:");
    let completes = count(&c.nodes[acker], "rejoin complete");
    c.nodes[behind].signal(Signal::SIGSTOP);
    let job = inserted(&on_leader.put(b"acknowledged by two"));
    drop(on_leader);
    c.nodes[acker].kill9();
    c.nodes[acker].wipe();
    c.nodes[l].kill9();
    c.nodes[acker].start(&[]);
    let marker = c.nodes[acker].data_dir.join("rejoin");
    // P6-T3: discovery writes the marker only once it decides to rejoin
    // (here it cannot: one voter answers at most), and starts no Raft.
    assert!(
        c.nodes[acker].wait_log(
            "startup: waiting for the cluster's status",
            Duration::from_secs(10)
        ),
        "{}",
        c.nodes[acker].log_text()
    );
    c.nodes[behind].signal(Signal::SIGCONT);

    // `behind` lacks the job and `acker` refuses to vote: no leader.
    // (`/admin` is served only once a node accepts clients.)
    std::thread::sleep(Duration::from_secs(3));
    let a = c.nodes[behind].admin().unwrap();
    assert_ne!(a["cluster"]["role"], "leader", "{a}");
    assert_eq!(c.nodes[acker].readyz(), 503);
    assert!(c.nodes[acker].admin().is_none());
    assert_eq!(count(&c.nodes[acker], "rejoin mode:"), rejoins + 1);
    assert!(!marker.exists());
    assert!(!c.nodes[acker].has_raft_state());

    // A crash after discovery decided to rejoin keeps the marker (written
    // here by hand: that moment cannot be reached without a second voter):
    // still rejoining, and --cluster-init is refused.
    c.nodes[acker].kill9();
    std::fs::write(&marker, "rejoining\n").unwrap();
    let (st, _, err) = run(&[
        "--config",
        c.nodes[acker].config.to_str().unwrap(),
        "--cluster-init",
    ]);
    assert_eq!(st.code(), Some(1), "{err}");
    assert!(err.contains("rejoin marker"), "{err}");
    c.nodes[acker].start(&[]);
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(c.nodes[acker].readyz(), 503);
    assert_eq!(count(&c.nodes[acker], "rejoin mode:"), rejoins + 2);
    assert_ne!(
        c.nodes[behind].admin().unwrap()["cluster"]["role"],
        "leader"
    );

    // The old leader comes back: it wins (its log has the job), `acker`
    // catches up and leaves rejoin mode.
    c.nodes[l].start(&[]);
    c.wait_all_ready();
    assert!(!marker.exists());
    let a = c.nodes[acker].admin().unwrap();
    assert_eq!(a["cluster"]["rejoining"], false, "{a}");
    assert!(a["cluster"]["votes_refused"].as_u64().unwrap() > 0, "{a}");
    assert_eq!(count(&c.nodes[acker], "rejoin complete"), completes + 1);
    for n in &c.nodes {
        let mut cl = n.connect();
        let (hdr, body) = cl.body_reply(&format!("peek {job}"));
        assert_eq!(hdr, format!("FOUND {job} 19"), "node {}", n.id);
        assert_eq!(body, b"acknowledged by two");
    }

    // A wiped node started with --cluster-init by mistake finds a peer of
    // a running cluster and rejoins instead of bootstrapping.
    let nl = c.leader();
    let f = c.followers(nl)[0];
    let skip = c.nodes[f].log_text().lines().count();
    c.nodes[f].kill9();
    c.nodes[f].wipe();
    c.nodes[f].start(&["--cluster-init"]);
    assert!(c.nodes[f].wait_ready(Duration::from_secs(20)));
    let log = c.nodes[f].log_text();
    let since: Vec<&str> = log.lines().skip(skip).collect();
    assert!(
        since
            .iter()
            .any(|l| l.contains("already belongs to a running cluster")),
        "{log}"
    );
    assert!(since.iter().any(|l| l.contains("rejoin complete")), "{log}");
    let mut cl = c.nodes[f].connect();
    assert_eq!(cl.stat("stats", "current-jobs-ready"), "2");
}

/// A client that has seen its connection close and then sends a command on
/// another connection to the same node finds the first one gone
/// (docs/COMPAT.md C10). Each round races the close against the next
/// command, so a wrong order shows up within a few hundred rounds.
#[test]
fn a_close_is_ordered_before_later_commands_on_the_same_node() {
    let mut c = Cluster::start(3, &Opts::default());
    let l = c.leader();
    let f = c.followers(l)[0];
    for node in [l, f] {
        let mut p = c.nodes[node].connect();
        for round in 0..300 {
            let mut q = c.nodes[node].connect();
            q.send(b"quit\r\n");
            let (_, end) = q.read_to_end(Duration::from_secs(10));
            assert_eq!(end, End::Closed);
            assert_eq!(
                p.stat("stats", "current-connections"),
                "1",
                "node {node}, round {round}"
            );
        }
    }
}

/// Chaos finding 5: connection ids of a process whose `Connect`s never
/// reached the log were handed out again after a restart. Numbers now come
/// from durably reserved blocks.
#[test]
fn connection_ids_are_fresh_after_a_restart_without_committed_connects() {
    let mut c = Cluster::start(3, &Opts::default());
    let l = c.leader();
    let f = c.followers(l)[0];
    let next = |n: &Node| {
        n.admin().unwrap()["cluster"]["next_local_conn"]
            .as_u64()
            .unwrap()
    };
    let first = next(&c.nodes[f]);
    // No quorum: the `Connect`s of these connections are never committed.
    let others: Vec<usize> = (0..3).filter(|&i| i != f).collect();
    for &i in &others {
        c.nodes[i].signal(Signal::SIGSTOP);
    }
    let mut held = Vec::new();
    for _ in 0..3 {
        let mut cl = c.nodes[f].connect();
        cl.send(b"use t\r\n");
        held.push(cl);
    }
    // A connect returns once the kernel has queued the connection; the
    // node numbers it when its accept loop gets to it.
    let used = wait_for(Duration::from_secs(3), || {
        let n = next(&c.nodes[f]);
        (n == first + 3).then_some(n)
    })
    .unwrap_or_else(|| panic!("not all 3 connections numbered: {}", next(&c.nodes[f])));
    c.nodes[f].kill9();
    drop(held);
    for &i in &others {
        c.nodes[i].signal(Signal::SIGCONT);
    }
    c.nodes[f].start(&[]);
    assert!(c.nodes[f].wait_ready(Duration::from_secs(20)));
    let restarted = next(&c.nodes[f]);
    assert!(
        restarted >= used,
        "connection numbers reused: {restarted} after {used}"
    );
    let mut cl = c.nodes[f].connect();
    assert_eq!(cl.cmd("use t"), "USING t");
    assert_eq!(next(&c.nodes[f]), restarted + 1);
}

/// Security M4: a follower whose link to the leader is cut in one
/// direction only (the leader's replication still reaches it) closes its
/// clients after `node_timeout`, refuses new ones without queueing
/// anything, and the leader drops its connections after
/// `2 × node_timeout`.
#[test]
fn one_way_partition_isolates_the_follower_and_the_leader_drops_it() {
    let opts = Opts {
        node_timeout: "1s",
        proxied: true,
        ..Opts::default()
    };
    let mut c = Cluster::start(3, &opts);
    let l = c.leader();
    let f = c.followers(l)[0];
    let (lid, fid) = (c.nodes[l].id, c.nodes[f].id);

    let mut on_leader = c.nodes[l].connect();
    let job = inserted(&on_leader.put(b"x"));
    let mut worker = c.nodes[f].connect();
    // See `follower_kill_releases_its_reservations_after_drop_node`.
    let heard = Instant::now();
    assert_eq!(reserve(&mut worker, "reserve-with-timeout 5"), job);
    let mut waiter = c.nodes[l].connect();
    waiter.send(b"reserve-with-timeout 30\r\n");

    let cut = Instant::now();
    c.proxies[&(fid, lid)].cut();
    worker
        .stream
        .set_read_timeout(Some(Duration::from_millis(200)))
        .unwrap();
    let (_, end) = worker.read_to_end(Duration::from_secs(10));
    assert!(
        matches!(end, End::Closed | End::Error(_)),
        "the follower's client was not closed: {end:?}"
    );
    let mut buf = [0u8; 16];
    let took = cut.elapsed();
    assert!(took < Duration::from_secs(5), "{took:?}");
    // The leader dropped it: the job returns to ready and goes to the
    // waiting reserve.
    let (id, _) = read_reserved(&mut waiter);
    assert_eq!(id, job);
    let silent = heard.elapsed();
    assert!(silent >= Duration::from_secs(2), "{silent:?}");
    let a = c.nodes[l].admin().unwrap();
    assert!(
        a["cluster"]["drop_node_proposals"].as_u64().unwrap() >= 1,
        "{a}"
    );
    let a = c.nodes[f].admin().unwrap();
    assert_eq!(a["cluster"]["leader_id"], lid, "{a}");
    assert_eq!(a["cluster"]["isolated"], true, "{a}");
    // It still names the leader, but must not look ready to a load balancer.
    wait_for(Duration::from_secs(2), || {
        (c.nodes[f].readyz() == 503).then_some(())
    })
    .expect("an isolated node kept answering /readyz with 200");

    // Reconnecting to the isolated node creates no state: refused at
    // accept, nothing queued.
    for _ in 0..50 {
        if let Ok(s) = TcpStream::connect(c.nodes[f].client_addr()) {
            let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
            let _ = std::io::Read::read(&mut &s, &mut buf);
        }
    }
    let a = c.nodes[f].admin().unwrap();
    assert!(
        a["cluster"]["refused_connections"].as_u64().unwrap() >= 50,
        "{a}"
    );
    assert!(a["cluster"]["forward_queue"].as_u64().unwrap() <= 4, "{a}");

    c.proxies[&(fid, lid)].heal();
    wait_for(Duration::from_secs(10), || {
        (c.nodes[f].admin()?["cluster"]["isolated"] == false).then_some(())
    })
    .expect("the follower stayed isolated");
    let mut again = c.nodes[f].connect();
    let second = inserted(&again.put(b"y"));
    assert_eq!(
        on_leader.stat(&format!("stats-job {second}"), "state"),
        "ready"
    );
}

/// Security M3: under sustained load through a follower, inputs are not
/// resent (a stall resend only follows a leader change, an error, or proof
/// that something was dropped).
#[test]
fn sustained_load_through_a_follower_resends_nothing() {
    let mut c = Cluster::start(3, &Opts::default());
    let l = c.leader();
    let f = c.followers(l)[0];
    let addr = c.nodes[f].client_addr();
    let term0 = c.nodes[f].admin().unwrap()["cluster"]["term"].clone();
    let workers: Vec<_> = (0..16)
        .map(|w| {
            std::thread::spawn(move || {
                let s = TcpStream::connect(addr).unwrap();
                s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
                let mut cl = Proto::new(s);
                assert_eq!(cl.cmd(&format!("use t{w}")), format!("USING t{w}"));
                for i in 0..150 {
                    let id = inserted(&cl.put(format!("{w}-{i}").as_bytes()));
                    if i % 3 == 0 {
                        assert_eq!(cl.cmd(&format!("delete {id}")), "DELETED");
                    }
                }
            })
        })
        .collect();
    for w in workers {
        w.join().unwrap();
    }
    let a = wait_for(Duration::from_secs(10), || {
        let a = c.nodes[f].admin()?;
        (a["cluster"]["forward_queue"] == 0).then_some(a)
    })
    .expect("the forward queue never drained");
    let cl = &a["cluster"];
    // An election during the run (possible on a loaded machine) is a
    // legitimate reason to resend; overload alone is not.
    let causes: &[&str] = if cl["term"] == term0 {
        assert_eq!(cl["resent_inputs"], 0, "{a}");
        &["stall", "dropped", "error", "view"]
    } else {
        &["stall", "dropped"]
    };
    for cause in causes {
        assert_eq!(cl["forward_rewinds"][cause], 0, "{cause}: {a}");
    }
    let m = c.nodes[f].metrics();
    assert!(
        m.contains("beanstalkd_cluster_resent_inputs_total 0"),
        "{m}"
    );
    assert!(
        m.contains("beanstalkd_cluster_forward_rewinds_total{cause=\"stall\"} 0"),
        "{m}"
    );
}

/// The cluster CA and node certificates, plus `admin.pem` / `admin.key`
/// (SAN `bstk-admin`, client only) and `rogue-admin.*` from another CA.
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
    let ca = params.clone().self_signed(&ca_key).unwrap();
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
    let rogue_key = KeyPair::generate().unwrap();
    let rogue = params.self_signed(&rogue_key).unwrap();
    for (name, signer, signer_key) in [("admin", &ca, &ca_key), ("rogue-admin", &rogue, &rogue_key)]
    {
        let mut p =
            CertificateParams::new(vec![bstk_raft::tls::ADMIN_DNS_NAME.to_string()]).unwrap();
        p.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        p.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let key = KeyPair::generate().unwrap();
        let cert = p.signed_by(&key, signer, signer_key).unwrap();
        std::fs::write(dir.join(format!("{name}.pem")), cert.pem()).unwrap();
        std::fs::write(dir.join(format!("{name}.key")), key.serialize_pem()).unwrap();
    }
}

/// A TLS client config trusting `dir`'s cluster CA and presenting
/// `dir/<name>.pem`.
fn client_tls(dir: &Path, name: &str) -> Arc<rustls::ClientConfig> {
    use rustls_pki_types::pem::PemObject;
    let mut roots = rustls::RootCertStore::empty();
    for c in rustls_pki_types::CertificateDer::pem_file_iter(dir.join("cluster-ca.pem")).unwrap() {
        roots.add(c.unwrap()).unwrap();
    }
    let certs = rustls_pki_types::CertificateDer::pem_file_iter(dir.join(format!("{name}.pem")))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let key =
        rustls_pki_types::PrivateKeyDer::from_pem_file(dir.join(format!("{name}.key"))).unwrap();
    Arc::new(
        rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_client_auth_cert(certs, key)
        .unwrap(),
    )
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
    for n in &mut c.nodes {
        n.start(&["--cluster-init"]);
    }
    c.wait_all_ready();
    let l = c.leader();
    let f = c.followers(l)[0];
    let mut on_f = c.nodes[f].connect();
    let job = inserted(&on_f.put(b"secure"));
    let mut on_l = c.nodes[l].connect();
    assert_eq!(reserve(&mut on_l, "reserve-with-timeout 5"), job);

    // P6-T2: the admin certificate reads the membership; a node's
    // certificate, an admin certificate from another CA and plaintext are
    // refused on the admin channel.
    use bstk_raft::wire::{AdminRequest, AdminResponse};
    let port = c.nodes[f].cluster;
    let id = c.nodes[f].id;
    let admin = admin_request_with(
        port,
        Some((id, client_tls(pki.path(), "admin"))),
        AdminRequest::Membership,
    );
    match admin {
        Ok(Some(AdminResponse::Membership(s))) => {
            assert_eq!(s.membership.voters(), [1, 2, 3].into(), "{s:?}");
        }
        other => panic!("{other:?}"),
    }
    let node = format!("node{}", c.nodes[l].id);
    let refused = admin_request_with(
        port,
        Some((id, client_tls(pki.path(), &node))),
        AdminRequest::Membership,
    );
    assert!(
        matches!(&refused, Err(e) if e.contains("hello rejected")),
        "{refused:?}"
    );
    let rogue = admin_request_with(
        port,
        Some((id, client_tls(pki.path(), "rogue-admin"))),
        AdminRequest::Membership,
    );
    assert!(matches!(rogue, Ok(None) | Err(_)), "{rogue:?}");
    let plain = admin_request_with(port, None, AdminRequest::Membership);
    assert!(matches!(plain, Ok(None) | Err(_)), "{plain:?}");
}

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
        // Bootstrapping two voters (any number of seeds is fine without
        // --cluster-init).
        (
            single_node_config(
                dir.path(),
                plain,
                Some(
                    "[[cluster.peer]]\nid = 1\naddr = \"127.0.0.1:1\"\n\
                     [[cluster.peer]]\nid = 2\naddr = \"127.0.0.1:2\"\n",
                ),
            ),
            vec!["--cluster-init"],
            "3 or 5 voters",
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
        // An initial voter that is not a peer.
        (
            single_node_config(dir.path(), &format!("{plain}\ninitial_voters = [4]"), None),
            vec![],
            "node 4 is not listed in [[cluster.peer]]",
        ),
        // Timing.
        (
            single_node_config(dir.path(), &format!("{plain}\nheartbeat = \"500ms\""), None),
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

    let (st, _, err) = run(&["--config", &cfg, "--cluster-init"]);
    assert_eq!(st.code(), Some(1), "{err}");
    assert!(err.contains("already holds Raft state"), "{err}");
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn leader_among(c: &Cluster, among: &[usize]) -> usize {
    wait_for(Duration::from_secs(15), || {
        among.iter().copied().find(|&i| {
            c.nodes[i]
                .admin()
                .is_some_and(|a| a["cluster"]["role"] == "leader")
        })
    })
    .expect("no leader among the given nodes")
}

/// Chaos finding 6 at process level: a leader of an old term, cut off
/// while the others elected a new leader and committed a job, must not
/// commit anything through a node that lost its data and rejoins. The
/// rejoining node adopts the highest vote of the other nodes before it
/// starts Raft, so it rejects the stale leader.
///
/// The rejoin may finish (in a few milliseconds) before the new leader is
/// cut off. The rejoined node then holds X and may win a later term with
/// the old leader's vote, and the put sent to the old node is forwarded
/// to it and legitimately committed. So the test checks what the bug
/// breaks (the adopted vote, the term of any commit, X's body on every
/// node) rather than whether the put fails.
#[test]
fn stale_leader_is_rejected_by_a_rejoined_node() {
    const X_BODY: &[u8] = b"X, committed in the newer term";
    let opts = Opts {
        // No isolation of the stale leader's clients during the test.
        node_timeout: "30s",
        proxied: true,
        ..Opts::default()
    };
    let mut c = Cluster::start(3, &opts);
    let old = c.leader();
    let rest = c.followers(old);
    let old_id = c.nodes[old].id;
    let mut on_old = c.nodes[old].connect();
    inserted(&on_old.put(b"before"));

    c.cut_node(old_id);
    let new = leader_among(&c, &rest);
    let acker = rest.iter().copied().find(|&i| i != new).unwrap();
    let (new_id, acker_id) = (c.nodes[new].id, c.nodes[acker].id);
    let mut on_new = c.nodes[new].connect();
    let x = inserted(&on_new.put(X_BODY));
    drop(on_new);
    let new_term = c.nodes[new].admin().unwrap()["cluster"]["term"]
        .as_u64()
        .unwrap();
    let a = c.nodes[old].admin().unwrap();
    assert_eq!(
        a["cluster"]["role"], "leader",
        "the old leader stepped down: {a}"
    );
    assert!(
        a["cluster"]["term"].as_u64().unwrap() < new_term,
        "the old leader is not stale: {a}"
    );

    // The acker loses its data and rejoins; it reaches both others (the
    // old leader still cannot reach the new one). Then the new leader is
    // cut off: the stale leader and the rejoined node are alone. The
    // bootstrap may already have sent the acker through rejoin mode
    // (docs/DESIGN.md §8 "Bootstrap"), so its log is read from here.
    let skip = c.nodes[acker].log_text().lines().count();
    c.nodes[acker].kill9();
    c.nodes[acker].wipe();
    c.heal_pair(old_id, acker_id);
    c.nodes[acker].start(&[]);
    let adopted = wait_for(Duration::from_secs(20), || {
        c.nodes[acker]
            .log_text()
            .lines()
            .skip(skip)
            .find(|l| l.contains("rejoin: adopted the highest vote"))
            .map(str::to_owned)
    })
    .expect("the rejoining node never adopted a vote");
    // Tolerates color codes between `vote`, `=` and `T<term>` (logs are
    // plain off a terminal, but the parse must not depend on that).
    let adopted_term: u64 = adopted
        .rsplit_once("vote")
        .and_then(|(_, v)| v.split_once('T'))
        .and_then(|(_, v)| v.split('-').next())
        .and_then(|t| t.parse().ok())
        .unwrap_or_else(|| panic!("no adopted vote in {adopted:?}"));
    assert!(
        adopted_term >= new_term,
        "adopted a vote below the new leader's term {new_term}: {adopted}"
    );
    c.cut_node(new_id);
    c.heal_pair(old_id, acker_id);

    on_old
        .stream
        .set_read_timeout(Some(Duration::from_millis(200)))
        .unwrap();
    on_old.send(b"put 0 0 60 5\r\nstale\r\n");
    let (got, _) = on_old.read_to_end(Duration::from_secs(4));
    let got = String::from_utf8_lossy(&got).into_owned();
    // A put committed through a log holding X gets an id above X's; the
    // stale leader's state machine lacks X and would reuse X's id.
    let stale = got
        .strip_prefix("INSERTED ")
        .map(|s| s.trim_end().parse::<u64>().unwrap());
    if let Some(s) = stale {
        assert!(s > x, "the stale leader committed a put: {got:?}");
        let term = wait_for(Duration::from_secs(5), || {
            [old, acker]
                .iter()
                .filter_map(|&i| c.nodes[i].admin())
                .find(|a| a["cluster"]["role"] == "leader")
                .and_then(|a| a["cluster"]["term"].as_u64())
        });
        assert!(
            term.is_some_and(|t| t > new_term),
            "a put committed without a leader above term {new_term}: {term:?}"
        );
    }
    wait_for(Duration::from_secs(5), || {
        (c.nodes[old].admin()?["cluster"]["role"] != "leader").then_some(())
    })
    .expect("the stale leader still leads");

    c.heal_all();
    c.wait_all_ready();
    for n in &c.nodes {
        let mut cl = n.connect();
        // The body, not only the id: the bug overwrites X with a job that
        // reuses X's id.
        let (hdr, body) = cl.body_reply(&format!("peek {x}"));
        assert!(
            hdr.starts_with(&format!("FOUND {x} ")) && body == X_BODY,
            "node {}: {hdr} {:?}",
            n.id,
            String::from_utf8_lossy(&body)
        );
        if let Some(s) = stale {
            let (hdr, body) = cl.body_reply(&format!("peek {s}"));
            assert!(
                hdr.starts_with(&format!("FOUND {s} ")) && body == b"stale",
                "node {}: {hdr}",
                n.id
            );
        }
    }
    let log = c.nodes[acker].log_text();
    assert!(
        log.lines()
            .skip(skip)
            .any(|l| l.contains("rejoin complete")),
        "{log}"
    );
}

/// `--cluster-init` on a wiped node while the other nodes are down: it
/// must not bootstrap a new cluster; it waits, and rejoins once they are
/// back.
#[test]
fn cluster_init_on_a_wiped_node_waits_for_the_survivors_and_rejoins() {
    let mut c = Cluster::start(3, &Opts::default());
    let l = c.leader();
    let mut on_leader = c.nodes[l].connect();
    let job = inserted(&on_leader.put(b"survives"));
    drop(on_leader);
    let f = c.followers(l)[0];
    // See `wiped_node_does_not_vote_until_it_has_caught_up`.
    let skip = c.nodes[f].log_text().lines().count();
    for i in 0..3 {
        c.nodes[i].kill9();
    }
    c.nodes[f].wipe();
    c.nodes[f].start(&["--cluster-init"]);
    std::thread::sleep(Duration::from_secs(3));
    let log = c.nodes[f].log_text();
    assert!(
        log.contains("--cluster-init: waiting for the other nodes' status"),
        "{log}"
    );
    assert!(!c.nodes[f].has_raft_state());
    assert_ne!(c.nodes[f].readyz(), 200);
    assert!(c.nodes[f].running());

    for i in (0..3).filter(|&i| i != f) {
        c.nodes[i].start(&[]);
    }
    c.wait_all_ready();
    let log = c.nodes[f].log_text();
    let since: Vec<&str> = log.lines().skip(skip).collect();
    assert!(
        since
            .iter()
            .any(|l| l.contains("already belongs to a running cluster")),
        "{log}"
    );
    assert!(since.iter().any(|l| l.contains("rejoin complete")), "{log}");
    for n in &c.nodes {
        let mut cl = n.connect();
        let (hdr, body) = cl.body_reply(&format!("peek {job}"));
        assert_eq!(hdr, format!("FOUND {job} 8"), "node {}", n.id);
        assert_eq!(body, b"survives");
    }
}

/// Bootstrap with `--cluster-init` on every node, in the orders the race
/// can take: (1) node 1 initializes before the others can ask anyone (the
/// others then see its bootstrap-only state and initialize too); (2)
/// nodes 1 and 2 form the cluster and elect a leader before node 3 can ask
/// (node 3 then rejoins). Both converge to one working cluster. (Starting
/// every node at once, the common case, is what every other test does.)
#[test]
fn cluster_init_on_every_node_converges_in_any_order() {
    let opts = Opts {
        proxied: true,
        ..Opts::default()
    };

    // (1) Node 1 sees 2 and 3; they see nobody.
    let mut c = Cluster::configure(3, &opts);
    for (a, b) in [(2, 1), (2, 3), (3, 1), (3, 2)] {
        c.proxies[&(a, b)].cut();
    }
    for n in &mut c.nodes {
        n.start(&["--cluster-init"]);
    }
    wait_for(Duration::from_secs(20), || {
        c.nodes[0].has_raft_state().then_some(())
    })
    .expect("node 1 never initialized");
    std::thread::sleep(Duration::from_millis(500));
    for i in [1, 2] {
        assert!(
            !c.nodes[i].has_raft_state(),
            "node {} initialized without answers",
            i + 1
        );
    }
    c.heal_all();
    c.wait_all_ready();
    let l = c.leader();
    let mut cl = c.nodes[l].connect();
    let job = inserted(&cl.put(b"one"));
    for n in &c.nodes {
        let mut cl = n.connect();
        assert!(cl.body_reply(&format!("peek {job}")).0.starts_with("FOUND"));
        assert!(!n.log_text().contains("panicked"));
    }
    drop(c);

    // (2) Nodes 1 and 2 see everyone; node 3 answers but cannot ask.
    let mut c = Cluster::configure(3, &opts);
    for (a, b) in [(3, 1), (3, 2)] {
        c.proxies[&(a, b)].cut();
    }
    for n in &mut c.nodes {
        n.start(&["--cluster-init"]);
    }
    let l = leader_among(&c, &[0, 1]);
    let mut cl = c.nodes[l].connect();
    let job = inserted(&cl.put(b"two"));
    drop(cl);
    assert!(!c.nodes[2].has_raft_state());
    c.heal_all();
    c.wait_all_ready();
    let log = c.nodes[2].log_text();
    assert!(
        log.contains("already belongs to a running cluster"),
        "{log}"
    );
    assert!(log.contains("rejoin complete"), "{log}");
    let mut on3 = c.nodes[2].connect();
    assert!(
        on3.body_reply(&format!("peek {job}"))
            .0
            .starts_with("FOUND")
    );
}

/// A wiped node's first connection number is above the time floor, so
/// above anything its lost process could have handed out.
#[test]
fn wiped_node_numbers_connections_above_the_time_floor() {
    let mut c = Cluster::start(3, &Opts::default());
    let l = c.leader();
    let f = c.followers(l)[0];
    let next = |n: &Node| {
        n.admin().unwrap()["cluster"]["next_local_conn"]
            .as_u64()
            .unwrap()
    };
    let before = next(&c.nodes[f]);
    let mut held = Vec::new();
    for _ in 0..5 {
        let mut cl = c.nodes[f].connect();
        assert_eq!(cl.cmd("use t"), "USING t");
        held.push(cl);
    }
    let used = next(&c.nodes[f]);
    assert_eq!(used, before + 5);
    c.nodes[f].kill9();
    drop(held);
    c.nodes[f].wipe();
    std::thread::sleep(Duration::from_millis(1100));
    let floor = unix_now() << 16;
    c.nodes[f].start(&[]);
    assert!(c.nodes[f].wait_ready(Duration::from_secs(20)));
    let after = next(&c.nodes[f]);
    assert!(after > used, "{after} <= {used}");
    assert!(after >= floor, "{after} below the time floor {floor}");
    let mut cl = c.nodes[f].connect();
    assert_eq!(cl.cmd("use t"), "USING t");
}

/// A single-node cluster whose data directory is empty cannot rejoin (it
/// has nobody to learn its state from): a clear error, not a hang.
#[test]
fn single_node_cannot_rejoin() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    let cfg = single_node_config(dir.path(), "insecure_plaintext = true", None);
    let (st, _, err) = run(&["--config", &cfg]);
    assert_eq!(st.code(), Some(1), "{err}");
    assert!(err.contains("cannot rejoin"), "{err}");
}

/// One request on a plaintext admin connection (protocol version 4) to
/// the cluster port `port`; `None` if the node does not answer.
fn admin_request(
    port: u16,
    body: bstk_raft::wire::AdminRequest,
) -> Option<bstk_raft::wire::AdminResponse> {
    admin_request_with(port, None, body).unwrap_or_else(|e| panic!("{e}"))
}

/// Like [`admin_request`], over TLS as node `tls.0` expects (`tls.1`
/// presents the client certificate). `Err`: the hello was refused.
fn admin_request_with(
    port: u16,
    tls: Option<(u64, Arc<rustls::ClientConfig>)>,
    body: bstk_raft::wire::AdminRequest,
) -> Result<Option<bstk_raft::wire::AdminResponse>, String> {
    use bstk_raft::wire::{self, AdminHello, ClientMsg, ServerHello, ServerMsg};
    trait Io: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin {}
    impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin> Io for T {}
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let Ok(tcp) = tokio::net::TcpStream::connect(("127.0.0.1", port)).await else {
            return Ok(None);
        };
        let mut s: Box<dyn Io> = match tls {
            None => Box::new(tcp),
            Some((id, c)) => Box::new(
                tokio_rustls::TlsConnector::from(c)
                    .connect(bstk_raft::tls::server_name_for(id).unwrap(), tcp)
                    .await
                    .map_err(|e| format!("TLS handshake: {e}"))?,
            ),
        };
        let hello = ClientMsg::AdminHello(AdminHello {
            version: wire::PROTOCOL_VERSION,
            to: None,
        });
        let f = wire::encode(&hello, wire::DEFAULT_MAX_FRAME).unwrap();
        if wire::write_frame(&mut s, &f).await.is_err() {
            return Ok(None);
        }
        match wire::read_frame::<_, ServerMsg>(&mut s, wire::DEFAULT_MAX_FRAME).await {
            Ok(Some(ServerMsg::Hello(ServerHello::Accepted { .. }))) => {}
            Ok(None) => return Ok(None),
            other => return Err(format!("admin hello refused: {other:?}")),
        }
        let f = wire::encode(&ClientMsg::Admin { id: 1, body }, wire::DEFAULT_MAX_FRAME).unwrap();
        if wire::write_frame(&mut s, &f).await.is_err() {
            return Ok(None);
        }
        match wire::read_frame::<_, ServerMsg>(&mut s, wire::DEFAULT_MAX_FRAME).await {
            Ok(Some(ServerMsg::Admin { id: 1, body })) => Ok(Some(body)),
            _ => Ok(None),
        }
    })
}

fn admin_status(port: u16) -> Option<bstk_raft::status::NodeStatusEx> {
    match admin_request(port, bstk_raft::wire::AdminRequest::Membership)? {
        bstk_raft::wire::AdminResponse::Membership(s) => Some(*s),
        other => panic!("membership expected: {other:?}"),
    }
}

/// P6-T2: the admin channel's membership read reports each node's view,
/// while Raft runs and before it does (a node waiting for its rejoin
/// probes). P6-T4: a change sent to a follower names the leader, one with
/// a stale `expect` is a conflict, and before Raft runs there is no leader.
#[test]
fn admin_status_reports_the_membership_before_and_after_raft_runs() {
    use bstk_raft::wire::{AdminRequest, AdminResponse};
    let mut c = Cluster::start(3, &Opts::default());
    let l = c.leader();
    c.wait_all_ready();
    let leader_id = c.nodes[l].id;
    let mut log_id = None;
    for n in &c.nodes {
        let s = wait_for(Duration::from_secs(10), || {
            admin_status(n.cluster).filter(|s| s.leader == Some(leader_id))
        })
        .unwrap_or_else(|| panic!("node {} never reported the leader", n.id));
        assert!(s.raft_running && !s.rejoining, "{s:?}");
        assert!(s.term >= 1 && s.status.has_state, "{s:?}");
        assert!(s.last_applied.is_some(), "{s:?}");
        let m = &s.membership;
        assert_eq!(m.voters(), [1, 2, 3].into(), "{s:?}");
        assert!(
            m.learners().is_empty() && !m.is_joint() && m.committed,
            "{s:?}"
        );
        assert_eq!(m.nodes[&n.id], format!("127.0.0.1:{}", n.cluster));
        assert_eq!(s.highest_member, 3);
        assert!(log_id.is_none() || log_id == m.log_id, "{s:?}");
        log_id = m.log_id;
    }
    let stale = log_id.map(|mut l| {
        l.index += 1;
        l
    });
    let changes = |expect| {
        [
            AdminRequest::AddLearner {
                id: 4,
                addr: "127.0.0.1:1".into(),
                expect,
            },
            AdminRequest::Promote {
                ids: [3].into(),
                expect,
                force: false,
            },
            AdminRequest::Remove {
                id: 3,
                expect,
                force: false,
            },
            AdminRequest::SetAddr {
                id: 3,
                addr: "127.0.0.1:1".into(),
                expect,
                force: false,
            },
        ]
    };
    let f = c.followers(l)[0];
    for req in changes(log_id) {
        assert_eq!(
            admin_request(c.nodes[f].cluster, req),
            Some(AdminResponse::NotLeader {
                leader: Some(leader_id),
                addr: Some(format!("127.0.0.1:{}", c.nodes[l].cluster)),
            })
        );
    }
    for req in changes(stale) {
        assert_eq!(
            admin_request(c.nodes[l].cluster, req),
            Some(AdminResponse::Conflict { current: log_id })
        );
    }
    assert_eq!(
        admin_status(c.nodes[l].cluster).unwrap().membership.log_id,
        log_id
    );

    // Node 1 alone, in rejoin mode with its state kept (as after a crash
    // during a rejoin): its probes find nobody, so Raft never starts, and
    // the answer comes from storage.
    for n in &mut c.nodes {
        n.stop(Signal::SIGTERM);
    }
    std::fs::write(c.nodes[0].data_dir.join("rejoin"), b"rejoining\n").unwrap();
    c.nodes[0].start(&[]);
    let s = wait_for(Duration::from_secs(10), || admin_status(c.nodes[0].cluster))
        .expect("no answer before Raft runs");
    assert!(
        !s.raft_running && s.rejoining && s.leader.is_none(),
        "{s:?}"
    );
    assert_eq!(s.membership.voters(), [1, 2, 3].into(), "{s:?}");
    assert_eq!(s.membership.log_id, log_id, "{s:?}");
    assert_eq!(s.highest_member, 3, "{s:?}");
    assert_eq!(s.term, s.status.vote.unwrap().leader_id().term, "{s:?}");
    for req in changes(log_id) {
        assert_eq!(
            admin_request(c.nodes[0].cluster, req),
            Some(AdminResponse::NotLeader {
                leader: None,
                addr: None
            })
        );
    }
}

// P6-T4: membership changes through the admin channel (a minimal client;
// the operator CLI is P6-T5).

type Expect = Option<openraft::LogId<u64>>;
use bstk_raft::wire::{AdminRequest as Req, AdminResponse as Resp};

fn admin(port: u16, body: Req) -> Resp {
    admin_request(port, body).expect("the admin channel did not answer")
}

/// The membership log id node `port` reports.
fn current(port: u16) -> Expect {
    admin_status(port).expect("status").membership.log_id
}

fn add(id: u64, addr: &str, expect: Expect) -> Req {
    Req::AddLearner {
        id,
        addr: addr.into(),
        expect,
    }
}

fn promote(ids: &[u64], expect: Expect, force: bool) -> Req {
    Req::Promote {
        ids: ids.iter().copied().collect(),
        expect,
        force,
    }
}

fn remove(id: u64, expect: Expect, force: bool) -> Req {
    Req::Remove { id, expect, force }
}

fn set_addr(id: u64, addr: &str, expect: Expect, force: bool) -> Req {
    Req::SetAddr {
        id,
        addr: addr.into(),
        expect,
        force,
    }
}

fn refused(r: &Resp, text: &str) -> bool {
    matches!(r, Resp::Refused { reason } if reason.contains(text))
}

/// Sends `make(current membership)` to the leader at `port` until it is
/// accepted, retrying the refusals that clear by themselves (a learner
/// still catching up or rejoining, a change in progress) for up to 30 s;
/// the note of the `Done` or `Started` answer.
fn change(port: u16, make: impl Fn(Expect) -> Req) -> Option<String> {
    let transient = [
        "not caught up",
        "has not replicated",
        "rejoining",
        "in progress",
        "did not answer",
    ];
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let req = make(current(port));
        match admin(port, req.clone()) {
            Resp::Done { note, .. } | Resp::Started { note } => return note,
            r @ Resp::Refused { .. } if transient.iter().any(|t| refused(&r, t)) => {
                assert!(Instant::now() < deadline, "{req:?}: {r:?}");
                std::thread::sleep(Duration::from_millis(200));
            }
            r => panic!("{req:?}: {r:?}"),
        }
    }
}

/// Waits until every running node in `among` reports the committed uniform
/// membership `voters` + `learners`.
fn wait_membership(c: &mut Cluster, among: &[usize], voters: &[u64], learners: &[u64]) {
    let (voters, learners): (BTreeSet<u64>, BTreeSet<u64>) = (
        voters.iter().copied().collect(),
        learners.iter().copied().collect(),
    );
    for &i in among {
        if !c.nodes[i].running() {
            continue;
        }
        wait_for(Duration::from_secs(30), || {
            let m = admin_status(c.nodes[i].cluster)?.membership;
            (m.committed && !m.is_joint() && m.voters() == voters && m.learners() == learners)
                .then_some(())
        })
        .unwrap_or_else(|| {
            panic!(
                "node {} never reported voters {voters:?} learners {learners:?}: {:?}",
                c.nodes[i].id,
                admin_status(c.nodes[i].cluster)
            )
        });
    }
}

/// Adds node `n` as a learner through the leader at `port` and starts it.
fn add_and_start(c: &mut Cluster, port: u16, n: usize) {
    let id = c.nodes[n].id;
    let addr = format!("127.0.0.1:{}", c.nodes[n].cluster);
    assert_eq!(change(port, |e| add(id, &addr, e)), None);
    c.nodes[n].start(&[]);
    assert!(
        c.nodes[n].wait_ready(Duration::from_secs(30)),
        "node {id} never became ready"
    );
}

/// P6-T4: grow 3 -> 5 one voter at a time (add a learner, start it, promote
/// it) and shrink back to 3, through the admin channel; the guardrails that
/// need no fault: one voter per promote, only learners are promoted, ids
/// never reused, no fewer than 3 voters without force, no promotion of a
/// learner that has not caught up, concurrent requests.
#[test]
fn admin_grows_three_to_five_and_shrinks_back() {
    let opts = Opts {
        node_timeout: "2s",
        ..Opts::default()
    };
    let mut c = Cluster::start(3, &opts);
    let l = c.leader();
    let lp = c.nodes[l].cluster;
    let n4 = c.configure_extra_node(&[1, 2, 3, 4], &[], &opts);
    let n5 = c.configure_extra_node(&[1, 2, 3, 5], &[], &opts);
    add_and_start(&mut c, lp, n4);
    add_and_start(&mut c, lp, n5);
    wait_membership(&mut c, &[0, 1, 2, n4, n5], &[1, 2, 3], &[4, 5]);

    // One voter per step; never a node that is not a learner (A1).
    let r = admin(lp, promote(&[4, 5], current(lp), true));
    assert!(refused(&r, "one node at a time"), "{r:?}");
    let r = admin(lp, promote(&[6], current(lp), true));
    assert!(refused(&r, "not a learner"), "{r:?}");

    let note = change(lp, |e| promote(&[4], e, false)).expect("a note");
    assert!(note.contains("4 voters"), "{note}");
    assert_eq!(change(lp, |e| promote(&[5], e, false)), None);
    wait_membership(&mut c, &[0, 1, 2, n4, n5], &[1, 2, 3, 4, 5], &[]);
    // Promoting a voter again: nothing to do.
    let cur = current(lp);
    assert_eq!(
        admin(lp, promote(&[5], cur, false)),
        Resp::Done {
            log_id: cur,
            note: None
        }
    );
    let mut on_5 = c.nodes[n5].connect();
    let job = inserted(&on_5.put(b"five"));
    let mut on_l = c.nodes[l].connect();
    assert_eq!(reserve(&mut on_l, "reserve-with-timeout 5"), job);
    assert_eq!(on_l.cmd(&format!("delete {job}")), "DELETED");
    drop(on_5);

    // Shrink: each removed node is told to be stopped.
    for n in [n5, n4] {
        let id = c.nodes[n].id;
        let note = change(lp, |e| remove(id, e, false)).expect("a note");
        assert!(note.contains("stop its process"), "{note}");
        c.nodes[n].stop(Signal::SIGTERM);
    }
    wait_membership(&mut c, &[0, 1, 2], &[1, 2, 3], &[]);
    assert_eq!(admin_status(lp).unwrap().highest_member, 5);

    // Ids are never reused, nor skipped ones readmitted.
    for id in [5, 4, 2] {
        let r = admin(lp, add(id, "127.0.0.1:1", current(lp)));
        assert!(
            refused(&r, "never reused") || refused(&r, "already a member"),
            "{id}: {r:?}"
        );
    }
    // No fewer than 3 voters without force.
    let f0 = c.followers(l)[0];
    let r = admin(lp, remove(c.nodes[f0].id, current(lp), false));
    assert!(refused(&r, "fewer than 3"), "{r:?}");
    // A learner that never ran is not promoted (not even replicated to).
    assert_eq!(change(lp, |e| add(6, "127.0.0.1:1", e)), None);
    let r = admin(lp, promote(&[6], current(lp), false));
    assert!(refused(&r, "not replicated anything"), "{r:?}");
    assert!(change(lp, |e| remove(6, e, false)).is_some());

    // Concurrent requests against one membership: exactly one applies.
    let cur = current(lp);
    let answers: Vec<Resp> = std::thread::scope(|s| {
        let hs: Vec<_> = [7, 8]
            .into_iter()
            .map(|id| s.spawn(move || admin(lp, add(id, &format!("127.0.0.1:{id}"), cur))))
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let applied = answers
        .iter()
        .filter(|r| matches!(r, Resp::Done { .. } | Resp::Started { .. }))
        .count();
    assert_eq!(applied, 1, "{answers:?}");
    assert!(
        answers
            .iter()
            .any(|r| refused(r, "in progress") || matches!(r, Resp::Conflict { .. })),
        "{answers:?}"
    );
}

/// P6-T4: removing a follower that holds a reservation (below 3 voters
/// only with force): the job is released at once and the removed node's
/// client is closed.
#[test]
fn admin_removes_a_follower_holding_a_reservation() {
    let opts = Opts {
        node_timeout: "1s",
        ..Opts::default()
    };
    let mut c = Cluster::start(3, &opts);
    let l = c.leader();
    let lp = c.nodes[l].cluster;
    let fs = c.followers(l);
    let (f, keep) = (fs[0], fs[1]);
    let fid = c.nodes[f].id;
    let mut on_leader = c.nodes[l].connect();
    let job = inserted(&on_leader.put(b"x"));
    let mut worker = c.nodes[f].connect();
    assert_eq!(reserve(&mut worker, "reserve-with-timeout 5"), job);
    let mut waiter = c.nodes[keep].connect();
    waiter.send(b"reserve-with-timeout 30\r\n");

    let r = admin(lp, remove(fid, current(lp), false));
    assert!(refused(&r, "fewer than 3"), "{r:?}");
    let removed = Instant::now();
    let note = change(lp, |e| remove(fid, e, true)).expect("a note");
    assert!(
        note.contains("stop its process") && note.contains("below 3"),
        "{note}"
    );
    let (id, _) = read_reserved(&mut waiter);
    assert_eq!(id, job);
    assert!(
        removed.elapsed() < Duration::from_secs(3),
        "{:?}",
        removed.elapsed()
    );
    worker
        .stream
        .set_read_timeout(Some(Duration::from_millis(200)))
        .unwrap();
    let (_, end) = worker.read_to_end(Duration::from_secs(10));
    assert!(matches!(end, End::Closed | End::Error(_)), "{end:?}");
    c.nodes[f].stop(Signal::SIGTERM);
    let voters = [c.nodes[l].id, c.nodes[keep].id];
    wait_membership(&mut c, &[l, keep], &voters, &[]);
    let mut on_keep = c.nodes[keep].connect();
    inserted(&on_keep.put(b"y"));
}

/// P6-T4: the leader removes itself: the answer says to stop it, another
/// node leads, and the removed node's connections are closed out (its
/// reservation is released to a waiter on another node). The followers keep
/// admitting the removed leader until they know the removal is committed,
/// so they learn the commit from it, before any election; openraft 0.9
/// still needs an election afterwards (the old leader steps down without
/// handing over), so `node_timeout` must outlast one, and then the remaining
/// nodes' clients stay connected.
#[test]
fn admin_removes_the_leader() {
    let opts = Opts {
        node_timeout: "5s",
        ..Opts::default()
    };
    let mut c = Cluster::start(3, &opts);
    let l = c.leader();
    let lp = c.nodes[l].cluster;
    let lid = c.nodes[l].id;
    let fs = c.followers(l);
    let mut on_leader = c.nodes[l].connect();
    let job = inserted(&on_leader.put(b"held"));
    assert_eq!(reserve(&mut on_leader, "reserve-with-timeout 5"), job);
    let mut waiter = c.nodes[fs[0]].connect();
    waiter.send(b"reserve-with-timeout 30\r\n");
    let mut bystander = c.nodes[fs[1]].connect();
    let term = admin_status(lp).unwrap().term;

    let note = change(lp, |e| remove(lid, e, true)).expect("a note");
    let removal = current(lp);
    // The followers learn that the removal is committed from the old
    // leader, in its term (no election yet).
    for &f in &fs {
        let s = wait_for(Duration::from_millis(1000), || {
            admin_status(c.nodes[f].cluster).filter(|s| {
                s.membership.log_id == removal && s.membership.committed && s.term == term
            })
        });
        assert!(
            s.is_some(),
            "node {} did not learn the commit from the removed leader: {:?}",
            c.nodes[f].id,
            admin_status(c.nodes[f].cluster)
        );
    }
    assert!(
        note.contains("steps down") && note.contains("stop its process"),
        "{note}"
    );
    let (id, _) = read_reserved(&mut waiter);
    assert_eq!(id, job);
    let nl = wait_for(Duration::from_secs(15), || {
        fs.iter().copied().find(|&i| {
            c.nodes[i]
                .admin()
                .is_some_and(|a| a["cluster"]["role"] == "leader" && a["cluster"]["ready"] == true)
        })
    })
    .expect("no new leader");
    let voters: Vec<u64> = fs.iter().map(|&i| c.nodes[i].id).collect();
    wait_membership(&mut c, &fs, &voters, &[]);
    // A change on the removed node is answered with the new leader.
    let r = admin(lp, add(9, "127.0.0.1:1", None));
    assert!(
        matches!(r, Resp::NotLeader { leader, .. } if leader != Some(lid)),
        "{r:?}"
    );
    on_leader
        .stream
        .set_read_timeout(Some(Duration::from_millis(200)))
        .unwrap();
    let (_, end) = on_leader.read_to_end(Duration::from_secs(10));
    assert!(matches!(end, End::Closed | End::Error(_)), "{end:?}");
    c.nodes[l].stop(Signal::SIGTERM);
    // The remaining nodes' clients were never closed.
    let j = inserted(&bystander.put(b"bystander"));
    assert_eq!(bystander.cmd(&format!("delete {j}")), "DELETED");
    let mut on_new = c.nodes[nl].connect();
    let job = inserted(&on_new.put(b"after"));
    let other = fs.iter().copied().find(|&i| i != nl).unwrap();
    let mut on_other = c.nodes[other].connect();
    assert_eq!(reserve(&mut on_other, "reserve-with-timeout 10"), job);
}

/// P6-T4: replace a follower by a node with a new id (add, promote, remove
/// the old one), then move the new node to another address (`SetAddr`; no
/// other node's config names it, so they follow the membership) and restart
/// it there.
#[test]
fn admin_replaces_a_node_and_moves_it() {
    let opts = Opts {
        node_timeout: "2s",
        ..Opts::default()
    };
    let mut c = Cluster::start(3, &opts);
    let l = c.leader();
    let lp = c.nodes[l].cluster;
    let old = c.followers(l)[0];
    let old_id = c.nodes[old].id;
    let n4 = c.configure_extra_node(&[1, 2, 3, 4], &[], &opts);
    add_and_start(&mut c, lp, n4);
    change(lp, |e| promote(&[4], e, false));
    change(lp, |e| remove(old_id, e, false));
    c.nodes[old].stop(Signal::SIGTERM);
    let keep: Vec<usize> = (0..3).filter(|&i| i != old).chain([n4]).collect();
    let mut voters: Vec<u64> = keep.iter().map(|&i| c.nodes[i].id).collect();
    voters.sort_unstable();
    wait_membership(&mut c, &keep, &voters, &[]);

    // Move node 4.
    c.nodes[n4].stop(Signal::SIGTERM);
    let (was, port) = (c.nodes[n4].cluster, claim_port());
    c.nodes[n4].cluster = port;
    let text = std::fs::read_to_string(&c.nodes[n4].config).unwrap();
    std::fs::write(
        &c.nodes[n4].config,
        text.replace(&format!(":{was}\""), &format!(":{port}\"")),
    )
    .unwrap();
    let addr = format!("127.0.0.1:{port}");
    let r = admin(lp, set_addr(4, "10.1.2.3:1", current(lp), true));
    assert!(refused(&r, "loopback"), "{r:?}");
    assert_eq!(change(lp, |e| set_addr(4, &addr, e, false)), None);
    c.nodes[n4].start(&[]);
    assert!(
        c.nodes[n4].wait_ready(Duration::from_secs(30)),
        "the moved node never caught up at its new address"
    );
    let s = admin_status(lp).unwrap();
    assert_eq!(s.membership.nodes[&4], addr, "{s:?}");
    let mut on_4 = c.nodes[n4].connect();
    let job = inserted(&on_4.put(b"moved"));
    let mut on_l = c.nodes[l].connect();
    assert_eq!(reserve(&mut on_l, "reserve-with-timeout 5"), job);
    release(was);
}

// P6-T5: the operator CLI, `beanstalkd-rs cluster`, against real nodes.

/// The output of one `beanstalkd-rs cluster` run.
struct CliOut {
    code: Option<i32>,
    out: String,
    err: String,
}

impl CliOut {
    fn json(&self) -> Value {
        serde_json::from_str(&self.out).unwrap_or_else(|e| panic!("{e}: {}", self.out))
    }
}

/// Runs `scripts/mkcluster-certs.sh` into a fresh directory: a CA plus the
/// named node ids and `admin`.
fn script_pki(what: &[&str]) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let out = Command::new("bash")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../scripts/mkcluster-certs.sh"
        ))
        .arg(dir.path())
        .args(what)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "mkcluster-certs.sh: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    dir
}

/// The options of an mTLS cluster whose node certificates are `pki`'s.
fn tls_opts(pki: &Path) -> Opts {
    let p = pki.display();
    Opts {
        node_timeout: "2s",
        extra: format!(
            "[cluster.tls]\ncert = \"{p}/node{{id}}.pem\"\nkey = \"{p}/node{{id}}.key\"\n\
             ca = \"{p}/cluster-ca.pem\"\n"
        ),
        ..Opts::default()
    }
}

/// How the CLI is told which cluster to talk to.
#[derive(Clone)]
struct Operator(Vec<String>);

impl Operator {
    /// With the `bstk-admin` certificate of `pki`, seeded with `node`.
    fn tls(pki: &Path, node: &Node) -> Operator {
        let p = pki.display();
        Operator(
            [
                "--node".to_string(),
                format!("127.0.0.1:{}", node.cluster),
                "--ca".into(),
                format!("{p}/cluster-ca.pem"),
                "--cert".into(),
                format!("{p}/admin.pem"),
                "--key".into(),
                format!("{p}/admin.key"),
            ]
            .into(),
        )
    }

    fn plain(node: &Node) -> Operator {
        Operator(
            [
                "--node".to_string(),
                format!("127.0.0.1:{}", node.cluster),
                "--insecure-plaintext".into(),
            ]
            .into(),
        )
    }

    fn command(&self, cmd: &[&str]) -> Command {
        let mut c = Command::new(BIN);
        c.arg("cluster")
            .args(cmd)
            .args(&self.0)
            .stdin(Stdio::null());
        c
    }

    fn run(&self, cmd: &[&str]) -> CliOut {
        let out = self.command(cmd).output().unwrap();
        CliOut {
            code: out.status.code(),
            out: String::from_utf8_lossy(&out.stdout).into_owned(),
            err: String::from_utf8_lossy(&out.stderr).into_owned(),
        }
    }

    /// Runs `cmd` until it succeeds, retrying the refusals that clear by
    /// themselves (a learner still catching up, a change in progress).
    fn run_until_done(&self, cmd: &[&str]) -> CliOut {
        let deadline = Instant::now() + Duration::from_secs(40);
        loop {
            let o = self.run(cmd);
            let transient = [
                "not caught up",
                "has not replicated",
                "rejoining",
                "in progress",
                "did not answer",
                "conflict",
            ];
            if o.code == Some(1) && transient.iter().any(|t| o.err.contains(t)) {
                assert!(Instant::now() < deadline, "{cmd:?}: {}", o.err);
                std::thread::sleep(Duration::from_millis(300));
                continue;
            }
            assert_eq!(o.code, Some(0), "{cmd:?}: {}", o.err);
            return o;
        }
    }

    /// `status --json` until every node in the membership shows `voters`
    /// and `learners` (committed, not joint).
    fn wait_membership(&self, voters: &[u64], learners: &[u64]) -> Value {
        let want = |ids: &[u64]| Value::from(ids.to_vec());
        let mut last = String::new();
        let found = wait_for(Duration::from_secs(40), || {
            let o = self.run(&["status", "--json"]);
            last = format!("{} {}", o.out, o.err);
            let v: Value = serde_json::from_str(&o.out).ok()?;
            let m = &v["membership"];
            (v["ok"] == true
                && m["committed"] == true
                && m["joint"] == false
                && m["voters"] == want(voters)
                && m["learners"] == want(learners))
            .then_some(v)
        });
        found.unwrap_or_else(|| panic!("membership never became {voters:?} + {learners:?}: {last}"))
    }
}

/// P6-T5: the whole life of a membership through the CLI over mTLS with
/// certificates from `scripts/mkcluster-certs.sh`: `status` (text, JSON, and
/// seeded from a node's configuration file), `add` sent to a follower (which
/// names the leader), starting the new node, `promote` (with the even-count
/// note), `remove` (with the note to stop the node), and the identity
/// checks: a node certificate, a certificate from another CA, an untrusted
/// server and plaintext are all refused.
#[test]
fn cli_manages_membership_over_mtls() {
    let pki = script_pki(&["1", "2", "3", "4", "admin"]);
    let opts = tls_opts(pki.path());
    let mut c = Cluster::start(3, &opts);
    let l = c.leader();
    let f = c.followers(l)[0];
    let leader_id = c.nodes[l].id;
    let n4 = c.configure_extra_node(&[1, 2, 3, 4], &[], &opts);
    let via_follower = Operator::tls(pki.path(), &c.nodes[f]);

    let o = via_follower.run(&["status"]);
    assert_eq!(o.code, Some(0), "{}", o.err);
    assert!(o.out.contains("voters:   1 2 3"), "{}", o.out);
    assert!(o.out.contains("learners: -"), "{}", o.out);
    assert!(
        o.out.contains(&format!("leader: node {leader_id}")),
        "{}",
        o.out
    );
    assert_eq!(o.out.matches(" ok").count(), 3, "{}", o.out);
    let v = via_follower.run(&["status", "--json"]).json();
    assert_eq!(
        (&v["ok"], &v["leader"]),
        (&Value::Bool(true), &Value::from(leader_id))
    );
    assert_eq!(v["highest_member"], 3);
    let nodes = v["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 3);
    assert!(
        nodes
            .iter()
            .all(|n| n["reachable"] == true && n["lag"].is_u64()),
        "{v}"
    );
    assert_eq!(nodes.iter().filter(|n| n["role"] == "leader").count(), 1);

    // Seeds and the CA from a node's configuration file.
    let p = pki.path().display().to_string();
    let o = Command::new(BIN)
        .args(["cluster", "status", "--config"])
        .arg(&c.nodes[0].config)
        .args([
            "--cert",
            &format!("{p}/admin.pem"),
            "--key",
            &format!("{p}/admin.key"),
        ])
        .output()
        .unwrap();
    assert_eq!(
        o.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&o.stderr)
    );

    // Identity checks.
    let other_ca = script_pki(&["admin"]);
    let node_cert = |name: &str| {
        let mut op = via_follower.0.clone();
        let at = |flag: &str| op.iter().position(|a| a == flag).unwrap() + 1;
        let (cert, key) = (at("--cert"), at("--key"));
        op[cert] = format!("{p}/{name}.pem");
        op[key] = format!("{p}/{name}.key");
        Operator(op)
    };
    let o = node_cert("node1").run(&["status"]);
    assert_eq!(o.code, Some(2), "{}", o.err);
    assert!(o.err.contains("not an admin certificate"), "{}", o.err);
    let q = other_ca.path().display().to_string();
    let with = |ca: &str, dir: &str| {
        let mut op = via_follower.0.clone();
        for (flag, file) in [
            ("--ca", "cluster-ca.pem"),
            ("--cert", "admin.pem"),
            ("--key", "admin.key"),
        ] {
            let at = op.iter().position(|a| a == flag).unwrap() + 1;
            op[at] = format!("{}/{file}", if flag == "--ca" { ca } else { dir });
        }
        Operator(op)
    };
    // An admin certificate from another CA: the nodes refuse it.
    let o = with(&p, &q).run(&["status"]);
    assert_eq!(o.code, Some(3), "{}", o.err);
    assert!(
        o.err.contains("refused the client certificate") || o.err.contains("TLS handshake"),
        "{}",
        o.err
    );
    // The right certificate but another CA's trust: the nodes are not trusted.
    let o = with(&q, &p).run(&["status"]);
    assert_eq!(o.code, Some(3), "{}", o.err);
    assert!(o.err.contains("TLS handshake failed"), "{}", o.err);
    // Plaintext against an mTLS cluster, and no certificate at all.
    let plain = Operator::plain(&c.nodes[f]);
    assert_eq!(plain.run(&["status", "--timeout", "3s"]).code, Some(3));
    let o = Command::new(BIN)
        .args([
            "cluster",
            "status",
            "--node",
            &format!("127.0.0.1:{}", c.nodes[f].cluster),
        ])
        .output()
        .unwrap();
    assert_eq!(o.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&o.stderr).contains("--cert"));

    // Grow 3 -> 4 through a follower: it names the leader and the CLI
    // follows.
    let addr4 = format!("127.0.0.1:{}", c.nodes[n4].cluster);
    let o = via_follower.run_until_done(&["add", "4", &addr4]);
    assert!(o.err.contains("following to node"), "{}", o.err);
    assert!(o.out.contains("learners: 4"), "{}", o.out);
    c.nodes[n4].start(&[]);
    assert!(c.nodes[n4].wait_ready(Duration::from_secs(30)));
    let o = via_follower.run_until_done(&["promote", "4"]);
    assert!(o.out.contains("voters:   1 2 3 4"), "{}", o.out);
    assert!(
        o.out.contains("NOTE:") && o.out.contains("4 voters"),
        "{}",
        o.out
    );
    via_follower.wait_membership(&[1, 2, 3, 4], &[]);
    let mut on_4 = c.nodes[n4].connect();
    let job = inserted(&on_4.put(b"four"));
    let mut on_l = c.nodes[l].connect();
    assert_eq!(reserve(&mut on_l, "reserve-with-timeout 5"), job);
    drop((on_4, on_l));

    // Shrink back: the node is told to be stopped.
    let o = via_follower.run_until_done(&["remove", "4"]);
    assert!(o.out.contains("voters:   1 2 3"), "{}", o.out);
    assert!(
        o.out.contains("NOTE:") && o.out.contains("stop its process"),
        "{}",
        o.out
    );
    c.nodes[n4].stop(Signal::SIGTERM);
    let v = via_follower.wait_membership(&[1, 2, 3], &[]);
    assert_eq!(v["highest_member"], 4);
}

/// P6-T5 over plaintext (`--insecure-plaintext`, from loopback): the
/// refusals and exit statuses: a guardrail refusal and a usage error and an
/// unreachable cluster each have their own status, errors are JSON with
/// `--json`, and concurrent changes from several processes are all either
/// applied or refused as a conflict or a change in progress, never both
/// applied to one membership.
#[test]
fn cli_exit_statuses_and_concurrent_changes() {
    let opts = Opts {
        node_timeout: "2s",
        ..Opts::default()
    };
    let mut c = Cluster::start(3, &opts);
    let l = c.leader();
    let f = c.followers(l)[0];
    let op = Operator::plain(&c.nodes[l]);

    let o = op.run(&["status"]);
    assert_eq!(o.code, Some(0), "{}", o.err);
    assert!(o.out.contains("voters:   1 2 3"), "{}", o.out);

    // Refused by a guardrail: exit 1, the reason on stderr.
    let o = op.run(&["promote", "9"]);
    assert_eq!(o.code, Some(1), "{}", o.out);
    assert!(
        o.err.contains("refused") && o.err.contains("not a learner"),
        "{}",
        o.err
    );
    let fid = c.nodes[f].id.to_string();
    let o = op.run(&["remove", &fid]);
    assert_eq!(o.code, Some(1));
    assert!(o.err.contains("fewer than 3"), "{}", o.err);
    let o = op.run(&["add", "2", "127.0.0.1:1"]);
    assert_eq!(o.code, Some(1), "{}", o.err);
    // The same, as JSON on stdout.
    let o = op.run(&["remove", &fid, "--json"]);
    assert_eq!(o.code, Some(1));
    let v = o.json();
    assert_eq!(
        (&v["ok"], &v["kind"], &v["exit"]),
        (&false.into(), &"refused".into(), &1.into())
    );
    assert!(v["error"].as_str().unwrap().contains("fewer than 3"), "{v}");
    // Nothing was changed.
    op.wait_membership(&[1, 2, 3], &[]);

    // Usage: no node to ask.
    let o = Command::new(BIN)
        .args(["cluster", "status", "--insecure-plaintext"])
        .output()
        .unwrap();
    assert_eq!(o.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&o.stderr).contains("no node to ask"));

    // Unreachable: exit 3 within the timeout.
    let dead = claim_port();
    let started = Instant::now();
    let o = Command::new(BIN)
        .args([
            "cluster",
            "status",
            "--insecure-plaintext",
            "--timeout",
            "3s",
            "--node",
        ])
        .arg(format!("127.0.0.1:{dead}"))
        .output()
        .unwrap();
    release(dead);
    assert_eq!(
        o.status.code(),
        Some(3),
        "{}",
        String::from_utf8_lossy(&o.stderr)
    );
    assert!(started.elapsed() < Duration::from_secs(10));

    // Several processes at once, each from the same view: those that win
    // change the membership, the others are told why.
    let children: Vec<_> = [5u64, 6, 7]
        .into_iter()
        .map(|id| {
            op.command(&["add", &id.to_string(), &format!("127.0.0.1:{id}")])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap()
        })
        .collect();
    let mut applied = 0;
    for ch in children {
        let o = ch.wait_with_output().unwrap();
        let err = String::from_utf8_lossy(&o.stderr);
        match o.status.code() {
            Some(0) => applied += 1,
            Some(1) => assert!(
                err.contains("conflict") || err.contains("in progress"),
                "{err}"
            ),
            other => panic!("{other:?}: {err}"),
        }
    }
    assert!(applied >= 1);
    let v = op.run(&["status", "--json"]).json();
    assert_eq!(
        v["membership"]["learners"].as_array().unwrap().len(),
        applied,
        "{v}"
    );
}

/// P6-T1: membership changes through the test-only hook (feature
/// `test-hooks`, `cluster::test_hooks`; run by `scripts/check.sh` as
/// `cargo test -p bstk-server --features test-hooks --test cluster membership::`).
/// Nothing reachable by an operator changes membership before P6-T4.
#[cfg(feature = "test-hooks")]
mod membership {
    use super::*;

    impl Cluster {
        /// Runs a membership command on node `i` (the leader) through the
        /// test hook; returns the log index of its (last) entry.
        fn hook(&self, i: usize, cmd: &str) -> Result<u64, String> {
            let n = &self.nodes[i];
            let out = n.data_dir.join("test-membership.out");
            let _ = std::fs::remove_file(&out);
            let tmp = n.data_dir.join("test-membership.cmd.tmp");
            std::fs::write(&tmp, cmd).unwrap();
            std::fs::rename(&tmp, n.data_dir.join("test-membership.cmd")).unwrap();
            let answer = wait_for(Duration::from_secs(30), || {
                std::fs::read_to_string(&out).ok()
            })
            .unwrap_or_else(|| panic!("no answer to {cmd:?} from node {}", n.id));
            let _ = std::fs::remove_file(&out);
            match answer.split_once(' ') {
                Some(("ok", index)) => Ok(index.trim().parse().unwrap()),
                _ => Err(answer),
            }
        }

        /// Waits until every running node in `among` applied `index`.
        fn wait_applied(&self, among: &[usize], index: u64) {
            for &i in among {
                wait_for(Duration::from_secs(20), || {
                    let a = self.nodes[i].admin()?;
                    (a["cluster"]["applied_index"].as_u64()? >= index).then_some(())
                })
                .unwrap_or_else(|| panic!("node {} never applied {index}", self.nodes[i].id));
            }
        }
    }

    fn rejected_hellos(n: &Node, from: u64) -> usize {
        n.log_text()
            .matches(&format!("hello from node {from} rejected"))
            .count()
    }

    /// Grow 3 -> 4: add a learner (its address is known to the others only
    /// from the membership), start it, it catches up and serves clients;
    /// promote it; the cluster then needs it for a quorum after losing a
    /// node, so every other node accepted its hello.
    #[test]
    fn learner_added_then_promoted_serves_clients() {
        let opts = Opts {
            node_timeout: "2s",
            ..Opts::default()
        };
        let mut c = Cluster::start(3, &opts);
        let l = c.leader();
        // Node 4's config: every current node, itself, and one unbound id.
        let n4 = c.configure_extra_node(&[1, 2, 3, 4], &[5], &opts);
        let addr4 = format!("127.0.0.1:{}", c.nodes[n4].cluster);
        let idx = c.hook(l, &format!("add-learner 4 {addr4}")).unwrap();
        c.wait_applied(&[0, 1, 2], idx);
        // P6-T2: the admin channel reports the learner.
        let s = admin_status(c.nodes[l].cluster).unwrap();
        assert_eq!(s.membership.learners(), [4].into(), "{s:?}");
        assert_eq!(s.membership.nodes[&4], addr4);
        assert_eq!(s.highest_member, 4, "{s:?}");
        // Without --cluster-init and without state: started after it was
        // added (the runbook order), it catches up as a rejoining node.
        c.nodes[n4].start(&[]);
        assert!(
            c.nodes[n4].wait_ready(Duration::from_secs(30)),
            "the learner never became ready"
        );
        let mut on_leader = c.nodes[l].connect();
        let mut on_learner = c.nodes[n4].connect();
        let job = inserted(&on_learner.put(b"via-learner"));
        assert_eq!(reserve(&mut on_leader, "reserve-with-timeout 5"), job);
        assert_eq!(on_leader.cmd(&format!("delete {job}")), "DELETED");

        let idx = c.hook(l, "change-membership 1,2,3,4").unwrap();
        c.wait_applied(&[0, 1, 2, n4], idx);
        let s = admin_status(c.nodes[n4].cluster).unwrap();
        assert_eq!(s.membership.voters(), [1, 2, 3, 4].into(), "{s:?}");
        assert!(s.membership.learners().is_empty() && !s.membership.is_joint());
        let job = inserted(&on_learner.put(b"via-voter"));
        assert_eq!(reserve(&mut on_leader, "reserve-with-timeout 5"), job);
        for i in 0..3 {
            assert_eq!(rejected_hellos(&c.nodes[i], 4), 0, "node {}", i + 1);
        }

        // Four voters: losing the leader leaves exactly a quorum (3), so the
        // new leader needs node 4's vote, and node 4 must accept its log.
        c.nodes[l].kill9();
        let nl = c.leader();
        assert_ne!(nl, l);
        let mut on_new = c.nodes[nl].connect();
        let job = inserted(&on_new.put(b"after-failover"));
        let mut again = c.nodes[n4].connect();
        assert_eq!(reserve(&mut again, "reserve-with-timeout 10"), job);
    }

    /// A member missing from a node's config is reached through its
    /// membership address and admitted through the membership allowlist:
    /// node 4's config lacks node `f2`, and after the leader is gone only
    /// `f2` and node 4 remain as voters, so the vote and then the
    /// replication or forwards between them must use the membership.
    #[test]
    fn member_missing_from_config_is_reached_through_the_membership() {
        let opts = Opts {
            node_timeout: "2s",
            ..Opts::default()
        };
        let mut c = Cluster::start(3, &opts);
        let l = c.leader();
        let fs = c.followers(l);
        let (f1, f2) = (fs[0], fs[1]);
        let (lid, f1id, f2id) = (c.nodes[l].id, c.nodes[f1].id, c.nodes[f2].id);
        let n4 = c.configure_extra_node(&[lid, f1id, 4], &[], &opts);
        let addr4 = format!("127.0.0.1:{}", c.nodes[n4].cluster);
        let idx = c.hook(l, &format!("add-learner 4 {addr4}")).unwrap();
        c.wait_applied(&[0, 1, 2], idx);
        c.nodes[n4].start(&[]);
        assert!(c.nodes[n4].wait_ready(Duration::from_secs(30)));
        let idx = c.hook(l, "change-membership 1,2,3,4").unwrap();
        c.wait_applied(&[0, 1, 2, n4], idx);
        let idx = c
            .hook(l, &format!("change-membership {lid},{f2id},4"))
            .unwrap();
        c.wait_applied(&[l, f2, n4], idx);

        c.nodes[l].kill9();
        let nl = wait_for(Duration::from_secs(15), || {
            [f2, n4].into_iter().find(|&i| {
                c.nodes[i].admin().is_some_and(|a| {
                    a["cluster"]["role"] == "leader" && a["cluster"]["ready"] == true
                })
            })
        })
        .expect("no leader among the two remaining voters");
        let other = if nl == f2 { n4 } else { f2 };
        assert!(c.nodes[other].wait_ready(Duration::from_secs(15)));
        let mut on_4 = c.nodes[n4].connect();
        let mut on_f2 = c.nodes[f2].connect();
        let a = inserted(&on_4.put(b"from-4"));
        assert_eq!(reserve(&mut on_f2, "reserve-with-timeout 10"), a);
        let b = inserted(&on_f2.put(b"from-f2"));
        assert_eq!(reserve(&mut on_4, "reserve-with-timeout 10"), b);
        assert_eq!(rejected_hellos(&c.nodes[n4], f2id), 0);
        assert_eq!(rejected_hellos(&c.nodes[f2], 4), 0);
    }

    /// Removing a follower that holds a reservation: the leader drops the
    /// non-member's connections at once (the job goes back to ready), the
    /// removed node's hellos are refused with the specific reason, and it
    /// isolates (closes its clients).
    #[test]
    fn removed_follower_is_dropped_rejected_and_isolates() {
        let opts = Opts {
            node_timeout: "1s",
            ..Opts::default()
        };
        let mut c = Cluster::start(3, &opts);
        let l = c.leader();
        let fs = c.followers(l);
        let (f, keep) = (fs[0], fs[1]);
        let fid = c.nodes[f].id;

        let mut on_leader = c.nodes[l].connect();
        let job = inserted(&on_leader.put(b"x"));
        let mut worker = c.nodes[f].connect();
        assert_eq!(reserve(&mut worker, "reserve-with-timeout 5"), job);
        let mut waiter = c.nodes[l].connect();
        waiter.send(b"reserve-with-timeout 30\r\n");

        let voters = format!("{},{}", c.nodes[l].id, c.nodes[keep].id);
        let removed = Instant::now();
        c.hook(l, &format!("change-membership {voters}")).unwrap();
        let (id, _) = read_reserved(&mut waiter);
        assert_eq!(id, job);
        let took = removed.elapsed();
        assert!(took < Duration::from_secs(2), "{took:?}");
        let a = c.nodes[l].admin().unwrap();
        assert!(
            a["cluster"]["drop_node_proposals"].as_u64().unwrap() >= 1,
            "{a}"
        );

        worker
            .stream
            .set_read_timeout(Some(Duration::from_millis(200)))
            .unwrap();
        let (_, end) = worker.read_to_end(Duration::from_secs(10));
        assert!(
            matches!(end, End::Closed | End::Error(_)),
            "the removed node's client was not closed: {end:?}"
        );
        wait_for(Duration::from_secs(10), || {
            (c.nodes[f].admin()?["cluster"]["isolated"] == true).then_some(())
        })
        .expect("the removed node did not isolate");
        wait_for(Duration::from_secs(10), || {
            c.nodes[f]
                .log_text()
                .contains("this node is not a member of the cluster")
                .then_some(())
        })
        .expect("the removed node never logged the specific rejection");
        wait_for(Duration::from_secs(10), || {
            (rejected_hellos(&c.nodes[l], fid) + rejected_hellos(&c.nodes[keep], fid) > 0)
                .then_some(())
        })
        .expect("nobody rejected the removed node's hello");
        assert!(
            c.nodes[l]
                .log_text()
                .contains(&format!("node {fid} is not a member of the cluster")),
        );

        // The two remaining voters keep serving.
        let mut on_keep = c.nodes[keep].connect();
        let second = inserted(&on_keep.put(b"y"));
        assert_eq!(
            on_leader.stat(&format!("stats-job {second}"), "state"),
            "ready"
        );
        assert_eq!(on_leader.stat("stats", "current-connections"), "3");
    }

    /// A member that is in no other node's config moves to a new address
    /// (`SetNodes`, no config override anywhere): the leader follows the
    /// membership and reaches it there.
    #[test]
    fn address_change_through_set_nodes_is_followed() {
        let opts = Opts::default();
        let mut c = Cluster::start(3, &opts);
        let l = c.leader();
        let n4 = c.configure_extra_node(&[1, 2, 3, 4], &[5], &opts);
        let addr4 = format!("127.0.0.1:{}", c.nodes[n4].cluster);
        let idx = c.hook(l, &format!("add-learner 4 {addr4}")).unwrap();
        c.wait_applied(&[0, 1, 2], idx);
        c.nodes[n4].start(&[]);
        assert!(c.nodes[n4].wait_ready(Duration::from_secs(30)));

        // Stop it, move it, tell the membership, restart it.
        c.nodes[n4].stop(Signal::SIGTERM);
        let old = c.nodes[n4].cluster;
        let new = claim_port();
        c.nodes[n4].cluster = new;
        let text = std::fs::read_to_string(&c.nodes[n4].config).unwrap();
        std::fs::write(
            &c.nodes[n4].config,
            text.replace(&format!(":{old}\""), &format!(":{new}\"")),
        )
        .unwrap();
        let idx = c.hook(l, &format!("set-nodes 4=127.0.0.1:{new}")).unwrap();
        c.wait_applied(&[0, 1, 2], idx);
        c.nodes[n4].start(&[]);
        assert!(
            c.nodes[n4].wait_ready(Duration::from_secs(30)),
            "the moved node never caught up at its new address"
        );
        c.wait_applied(&[n4], idx);
        let mut on_leader = c.nodes[l].connect();
        let mut on_moved = c.nodes[n4].connect();
        let job = inserted(&on_moved.put(b"moved"));
        assert_eq!(reserve(&mut on_leader, "reserve-with-timeout 5"), job);
        release(old);
    }

    /// Shrinking to a single voter: `leader_reachable` counts the
    /// membership's voters, not the config's peers, so the last voter keeps
    /// serving after the removed nodes are gone.
    #[test]
    fn shrunk_to_one_voter_keeps_serving() {
        let opts = Opts {
            node_timeout: "1s",
            ..Opts::default()
        };
        let mut c = Cluster::start(3, &opts);
        let l = c.leader();
        let lid = c.nodes[l].id;
        let idx = c.hook(l, &format!("change-membership {lid}")).unwrap();
        c.wait_applied(&[l], idx);
        for f in c.followers(l) {
            c.nodes[f].kill9();
        }
        std::thread::sleep(Duration::from_secs(3));
        let a = c.nodes[l].admin().unwrap();
        assert_eq!(a["cluster"]["isolated"], false, "{a}");
        assert_eq!(a["cluster"]["role"], "leader", "{a}");
        let mut client = c.nodes[l].connect();
        let job = inserted(&client.put(b"alone"));
        assert_eq!(reserve(&mut client, "reserve-with-timeout 5"), job);
    }

    /// P6-T3: grow 1 -> 2 -> 3 with Join and promote. Node 2 is started
    /// before it is added (it waits as a joiner, then catches up as a
    /// learner); node 3 after (the runbook order), with a single seed that
    /// is not the leader. A one-voter cluster gains a second voter without
    /// any vote adoption by a voter (a wiped voter of a one-voter cluster
    /// could never rejoin).
    #[test]
    fn grow_one_to_three_through_join_and_promote() {
        let opts = Opts {
            node_timeout: "2s",
            ..Opts::default()
        };
        let mut c = Cluster::start(1, &opts);
        let l = c.leader();
        let n2 = c.configure_extra_node(&[1], &[], &opts);
        c.nodes[n2].start(&[]);
        assert!(
            c.nodes[n2].wait_log("join: node 2 is not a member", Duration::from_secs(15)),
            "{}",
            c.nodes[n2].log_text()
        );
        assert!(!c.nodes[n2].has_raft_state());
        assert_ne!(c.nodes[n2].readyz(), 200);
        let addr2 = format!("127.0.0.1:{}", c.nodes[n2].cluster);
        let idx = c.hook(l, &format!("add-learner 2 {addr2}")).unwrap();
        assert!(
            c.nodes[n2].wait_ready(Duration::from_secs(30)),
            "the joiner never became ready"
        );
        c.wait_applied(&[n2], idx);
        let idx = c.hook(l, "change-membership 1,2").unwrap();
        c.wait_applied(&[l, n2], idx);
        let s = admin_status(c.nodes[n2].cluster).unwrap();
        assert_eq!(s.membership.voters(), [1, 2].into(), "{s:?}");

        // Node 3: added first, then started, knowing only node 2 (not the
        // leader).
        assert_eq!(c.leader(), l);
        let n3 = c.configure_extra_node(&[2], &[], &opts);
        let addr3 = format!("127.0.0.1:{}", c.nodes[n3].cluster);
        let idx = c.hook(l, &format!("add-learner 3 {addr3}")).unwrap();
        c.wait_applied(&[l, n2], idx);
        c.nodes[n3].start(&[]);
        assert!(
            c.nodes[n3].wait_ready(Duration::from_secs(30)),
            "node 3 never became ready"
        );
        let idx = c.hook(l, "change-membership 1,2,3").unwrap();
        c.wait_applied(&[l, n2, n3], idx);
        let log3 = c.nodes[n3].log_text();
        assert!(log3.contains("rejoin complete"), "{log3}");

        let mut on_3 = c.nodes[n3].connect();
        let job = inserted(&on_3.put(b"grown"));
        let mut on_1 = c.nodes[l].connect();
        assert_eq!(reserve(&mut on_1, "reserve-with-timeout 5"), job);
        drop(on_1);
        // Three voters: the two joiners elect a leader without node 1.
        c.nodes[l].kill9();
        let nl = c.leader();
        assert!(nl == n2 || nl == n3, "{nl}");
        let mut on_new = c.nodes[nl].connect();
        let job = inserted(&on_new.put(b"after-failover"));
        let other = if nl == n2 { n3 } else { n2 };
        assert!(c.nodes[other].wait_ready(Duration::from_secs(15)));
        let mut on_other = c.nodes[other].connect();
        assert_eq!(reserve(&mut on_other, "reserve-with-timeout 10"), job);
    }

    /// Grows 3 -> 4 voters (learner, then promote) and returns node 4's
    /// index; node 4 is configured with `seeds` (and itself).
    fn grow_to_four(c: &mut Cluster, l: usize, seeds: &[u64], opts: &Opts) -> usize {
        let mut peers = seeds.to_vec();
        peers.push(4);
        let n4 = c.configure_extra_node(&peers, &[], opts);
        let addr4 = format!("127.0.0.1:{}", c.nodes[n4].cluster);
        c.hook(l, &format!("add-learner 4 {addr4}")).unwrap();
        c.nodes[n4].start(&[]);
        assert!(c.nodes[n4].wait_ready(Duration::from_secs(30)));
        let idx = c.hook(l, "change-membership 1,2,3,4").unwrap();
        c.wait_applied(&[0, 1, 2, n4], idx);
        n4
    }

    /// P6-T3: after 3 -> 4 voters, a wiped original follower (whose config
    /// lists only the original three) rejoins against the current voters:
    /// it learns node 4 from the membership and needs answers from two of
    /// the three other voters (`rejoin_answers(4) = 2`). With the other
    /// original follower down for good, those are the leader and node 4,
    /// which only the membership names.
    #[test]
    fn wiped_voter_rejoins_against_the_current_voters() {
        let opts = Opts {
            node_timeout: "2s",
            ..Opts::default()
        };
        let mut c = Cluster::start(3, &opts);
        let l = c.leader();
        let n4 = grow_to_four(&mut c, l, &[1, 2, 3], &opts);
        let f = c.followers(l).into_iter().find(|&i| i != n4).unwrap();
        let mut on_leader = c.nodes[l].connect();
        let job = inserted(&on_leader.put(b"before-wipe"));
        drop(on_leader);
        let o = c
            .followers(l)
            .into_iter()
            .find(|&i| i != n4 && i != f)
            .unwrap();
        let skip = c.nodes[f].log_text().lines().count();
        c.nodes[f].kill9();
        c.nodes[f].wipe();
        // With node 4 and the other original follower down, only the leader
        // answers: not enough.
        c.nodes[n4].kill9();
        c.nodes[o].kill9();
        c.nodes[f].start(&[]);
        assert!(
            c.nodes[f].wait_log(
                "1 of the 2 answers needed from the current voters",
                Duration::from_secs(20)
            ),
            "{}",
            c.nodes[f].log_text()
        );
        assert!(!c.nodes[f].has_raft_state());
        assert_ne!(c.nodes[f].readyz(), 200);
        c.nodes[n4].start(&[]);
        assert!(
            c.nodes[f].wait_ready(Duration::from_secs(30)),
            "the wiped voter never rejoined"
        );
        let log = c.nodes[f].log_text();
        let since: Vec<&str> = log.lines().skip(skip).collect();
        let adopted = since
            .iter()
            .find(|l| {
                l.contains("rejoin: adopted the highest vote of the current voters that answered")
            })
            .unwrap_or_else(|| panic!("{log}"));
        assert!(adopted.contains("{1, 2, 3, 4}"), "{adopted}");
        assert!(since.iter().any(|l| l.contains("rejoin complete")), "{log}");
        let mut on_f = c.nodes[f].connect();
        let (hdr, _) = on_f.body_reply(&format!("peek {job}"));
        assert_eq!(hdr, format!("FOUND {job} 11"));
    }

    /// P6-T3: a wiped voter whose only seed was removed (and is gone) waits,
    /// saying so, without guessing; once its config names a current member
    /// it rejoins.
    #[test]
    fn stale_seed_list_waits_until_a_current_member_is_configured() {
        let opts = Opts {
            node_timeout: "2s",
            ..Opts::default()
        };
        let mut c = Cluster::start(3, &opts);
        let l = c.leader();
        let fs = c.followers(l);
        let (gone, keep) = (fs[0], fs[1]);
        let gone_id = c.nodes[gone].id;
        // Node 4 knows only `gone` and itself.
        let n4 = grow_to_four(&mut c, l, &[gone_id], &opts);
        let voters: Vec<String> = [l, keep, n4]
            .iter()
            .map(|&i| c.nodes[i].id.to_string())
            .collect();
        let idx = c
            .hook(l, &format!("change-membership {}", voters.join(",")))
            .unwrap();
        c.wait_applied(&[l, keep, n4], idx);
        c.nodes[gone].kill9();

        c.nodes[n4].kill9();
        c.nodes[n4].wipe();
        let skip = c.nodes[n4].log_text().lines().count();
        c.nodes[n4].start(&[]);
        std::thread::sleep(Duration::from_secs(4));
        assert!(c.nodes[n4].running());
        assert!(!c.nodes[n4].has_raft_state());
        assert_ne!(c.nodes[n4].readyz(), 200);
        let log = c.nodes[n4].log_text();
        let since: Vec<&str> = log.lines().skip(skip).collect();
        assert!(
            since
                .iter()
                .any(|l| l.contains("startup: waiting for the cluster's status")
                    && l.contains("no node that answered knows a committed membership")),
            "{log}"
        );

        // Name a current member (the leader) as a seed and restart.
        c.nodes[n4].kill9();
        let leader_line = format!(
            "[[cluster.peer]]\nid = {}\naddr = \"127.0.0.1:{}\"\n",
            c.nodes[l].id, c.nodes[l].cluster
        );
        let text = std::fs::read_to_string(&c.nodes[n4].config).unwrap();
        std::fs::write(&c.nodes[n4].config, format!("{text}{leader_line}")).unwrap();
        c.nodes[n4].start(&[]);
        assert!(
            c.nodes[n4].wait_ready(Duration::from_secs(30)),
            "never rejoined after a current member was configured"
        );
        let log = c.nodes[n4].log_text();
        assert!(log.contains("rejoin complete"), "{log}");
    }

    /// P6-T3: a removed node restarted with an empty data directory refuses
    /// to start (ids are never reused) and creates no Raft state.
    #[test]
    fn removed_id_with_an_empty_data_dir_refuses_to_start() {
        let opts = Opts::default();
        let mut c = Cluster::start(3, &opts);
        let l = c.leader();
        let fs = c.followers(l);
        let (f, keep) = (fs[0], fs[1]);
        let voters = format!("{},{}", c.nodes[l].id, c.nodes[keep].id);
        let idx = c.hook(l, &format!("change-membership {voters}")).unwrap();
        c.wait_applied(&[l, keep], idx);
        c.nodes[f].kill9();
        c.nodes[f].wipe();
        c.nodes[f].start(&[]);
        let st = c.nodes[f]
            .wait_exit(Duration::from_secs(20))
            .expect("the removed node kept running");
        assert_eq!(st.code(), Some(1), "{}", c.nodes[f].log_text());
        let log = c.nodes[f].log_text();
        assert!(log.contains("never reused"), "{log}");
        assert!(!c.nodes[f].has_raft_state());
        // The cluster is unaffected.
        let mut on_keep = c.nodes[keep].connect();
        inserted(&on_keep.put(b"still-here"));
    }

    /// P6-T3: `--cluster-init` with `initial_voters = [1, 2, 3]` on four
    /// configured nodes: node 4 never initializes; it joins once added.
    #[test]
    fn cluster_init_with_initial_voters_and_a_joiner() {
        let opts = Opts {
            extra: "initial_voters = [1, 2, 3]".into(),
            ..Opts::default()
        };
        let mut c = Cluster::configure(4, &opts);
        for n in &mut c.nodes {
            n.start(&["--cluster-init"]);
        }
        for i in 0..3 {
            assert!(c.nodes[i].wait_ready(Duration::from_secs(20)));
        }
        let l = c.leader();
        assert!(
            c.nodes[3].wait_log("join: node 4 is not a member", Duration::from_secs(15)),
            "{}",
            c.nodes[3].log_text()
        );
        assert!(!c.nodes[3].has_raft_state());
        assert_ne!(c.nodes[3].readyz(), 200);
        let s = admin_status(c.nodes[l].cluster).unwrap();
        assert_eq!(s.membership.voters(), [1, 2, 3].into(), "{s:?}");
        assert!(s.membership.learners().is_empty());

        let addr4 = format!("127.0.0.1:{}", c.nodes[3].cluster);
        c.hook(l, &format!("add-learner 4 {addr4}")).unwrap();
        assert!(
            c.nodes[3].wait_ready(Duration::from_secs(30)),
            "the joiner never became ready"
        );
        let mut on_4 = c.nodes[3].connect();
        let job = inserted(&on_4.put(b"joined"));
        let mut on_l = c.nodes[l].connect();
        assert_eq!(reserve(&mut on_l, "reserve-with-timeout 5"), job);
    }

    /// P6-T3 review: a voter removed while it is still in rejoin mode (the
    /// test hook holds back its `DropNode`) gives up with a refusal instead
    /// of asking for a `DropNode` no leader would propose.
    #[test]
    fn voter_removed_while_rejoining_gives_up() {
        let opts = Opts {
            node_timeout: "2s",
            ..Opts::default()
        };
        let mut c = Cluster::start(3, &opts);
        let l = c.leader();
        let fs = c.followers(l);
        let (f, other) = (fs[0], fs[1]);
        let f_id = c.nodes[f].id;
        let skip = c.nodes[f].log_text().lines().count();
        c.nodes[f].kill9();
        c.nodes[f].wipe();
        std::fs::create_dir_all(&c.nodes[f].data_dir).unwrap();
        std::fs::write(c.nodes[f].data_dir.join("test-hold-rejoin"), "").unwrap();
        c.nodes[f].start(&[]);
        let since = |n: &Node| -> Vec<String> {
            n.log_text()
                .lines()
                .skip(skip)
                .map(str::to_string)
                .collect()
        };
        wait_for(Duration::from_secs(20), || {
            since(&c.nodes[f])
                .iter()
                .any(|l| l.contains("rejoin: adopted the highest vote"))
                .then_some(())
        })
        .unwrap_or_else(|| panic!("{}", c.nodes[f].log_text()));
        assert!(c.nodes[f].running());
        assert_ne!(c.nodes[f].readyz(), 200);

        let keep = format!("{},{}", c.nodes[l].id, c.nodes[other].id);
        let idx = c.hook(l, &format!("change-membership {keep}")).unwrap();
        c.wait_applied(&[l, other], idx);
        let st = c.nodes[f]
            .wait_exit(Duration::from_secs(30))
            .unwrap_or_else(|| panic!("still running: {}", c.nodes[f].log_text()));
        assert!(!st.success(), "{st:?}");
        let log = since(&c.nodes[f]);
        assert!(
            log.iter().any(
                |l| l.contains(&format!("cannot finish rejoining node {f_id}"))
                    && l.contains("never reused")
            ),
            "{}",
            log.join("\n")
        );
        // The two remaining voters serve on.
        let mut on_leader = c.nodes[l].connect();
        inserted(&on_leader.put(b"after-removal"));
    }

    /// P6-T4 (A2): while a voter is rejoining (held by the test hook),
    /// voter changes are refused, the rejoining voter cannot be removed,
    /// learner changes go through; once it has caught up the promote does.
    /// Then a voter that does not answer blocks voter changes, except its
    /// own removal.
    #[test]
    fn admin_voter_changes_wait_for_rejoining_and_silent_voters() {
        let opts = Opts {
            node_timeout: "2s",
            ..Opts::default()
        };
        let mut c = Cluster::start(3, &opts);
        let l = c.leader();
        let lp = c.nodes[l].cluster;
        let n4 = c.configure_extra_node(&[1, 2, 3, 4], &[], &opts);
        add_and_start(&mut c, lp, n4);
        let fs: Vec<usize> = c.followers(l).into_iter().filter(|&i| i != n4).collect();
        let (f, o) = (fs[0], fs[1]);
        let fid = c.nodes[f].id;
        let skip = c.nodes[f].log_text().lines().count();
        c.nodes[f].kill9();
        c.nodes[f].wipe();
        std::fs::create_dir_all(&c.nodes[f].data_dir).unwrap();
        let hold = c.nodes[f].data_dir.join("test-hold-rejoin");
        std::fs::write(&hold, "").unwrap();
        c.nodes[f].start(&[]);
        wait_for(Duration::from_secs(20), || {
            c.nodes[f]
                .log_text()
                .lines()
                .skip(skip)
                .any(|l| l.contains("rejoin: adopted the highest vote"))
                .then_some(())
        })
        .unwrap_or_else(|| panic!("{}", c.nodes[f].log_text()));
        wait_for(Duration::from_secs(10), || {
            admin_status(c.nodes[f].cluster)
                .filter(|s| s.rejoining && s.raft_running)
                .map(|_| ())
        })
        .expect("never reported rejoining with Raft running");

        let cur = current(lp);
        let r = admin(lp, promote(&[4], cur, true));
        assert!(refused(&r, &format!("voter {fid} is rejoining")), "{r:?}");
        let r = admin(lp, remove(c.nodes[o].id, cur, true));
        assert!(refused(&r, &format!("voter {fid} is rejoining")), "{r:?}");
        let r = admin(lp, remove(fid, cur, true));
        assert!(refused(&r, "stop it before removing it"), "{r:?}");
        // Learner changes: a quorum of the same voters commits them, and
        // the rejoining node's re-check decides again on a newer membership.
        assert_eq!(change(lp, |e| add(5, "127.0.0.1:1", e)), None);
        assert!(change(lp, |e| remove(5, e, false)).is_some());

        std::fs::remove_file(&hold).unwrap();
        assert!(c.nodes[f].wait_ready(Duration::from_secs(30)));
        change(lp, |e| promote(&[4], e, false));
        wait_membership(&mut c, &[0, 1, 2, n4], &[1, 2, 3, 4], &[]);

        // A voter that does not answer: other voter changes wait; its own
        // removal (the usual reason to remove a voter) does not.
        let oid = c.nodes[o].id;
        c.nodes[o].kill9();
        let r = admin(lp, remove(4, current(lp), false));
        assert!(refused(&r, &format!("voter {oid} did not answer")), "{r:?}");
        let note = change(lp, |e| remove(oid, e, false)).expect("a note");
        assert!(note.contains("stop its process"), "{note}");
        let mut voters: Vec<u64> = [l, f, n4].iter().map(|&i| c.nodes[i].id).collect();
        voters.sort_unstable();
        wait_membership(&mut c, &[l, f, n4], &voters, &[]);
    }

    /// P6-T4 (A1): a joint configuration left behind by a leader that died
    /// between openraft's two steps is finished by the next leader towards
    /// the new voters, never back. The test hook holds the promote after its
    /// checks (meanwhile a second request is refused); the leader's links to
    /// the two original followers then deliver its requests but drop the
    /// answers, so they append the joint entry but it never commits on the
    /// leader, which is then killed.
    #[test]
    fn admin_joint_leftover_is_finished_by_the_next_leader() {
        let opts = Opts {
            node_timeout: "5s",
            proxied: true,
            ..Opts::default()
        };
        let mut c = Cluster::start(3, &opts);
        let l = c.leader();
        let lp = c.nodes[l].cluster;
        let lid = c.nodes[l].id;
        let n4 = c.configure_extra_node(&[1, 2, 3, 4], &[], &opts);
        add_and_start(&mut c, lp, n4);
        let fs: Vec<usize> = c.followers(l).into_iter().filter(|&i| i != n4).collect();
        let hold = c.nodes[l].data_dir.join("test-hold-change");
        std::fs::write(&hold, "").unwrap();
        let cur = current(lp);
        let held = std::thread::spawn(move || admin(lp, promote(&[4], cur, false)));
        assert!(
            c.nodes[l].wait_log(
                "test hook: holding the membership change",
                Duration::from_secs(20)
            ),
            "{}",
            c.nodes[l].log_text()
        );
        let r = admin(lp, add(5, "127.0.0.1:1", cur));
        assert!(refused(&r, "in progress"), "{r:?}");
        let r = held.join().unwrap();
        assert!(matches!(r, Resp::Started { .. }), "{r:?}");

        for &f in &fs {
            c.proxies[&(lid, c.nodes[f].id)].mute_replies();
        }
        std::fs::remove_file(&hold).unwrap();
        for &f in &fs {
            wait_for(Duration::from_secs(15), || {
                admin_status(c.nodes[f].cluster)
                    .filter(|s| s.membership.is_joint())
                    .map(|_| ())
            })
            .unwrap_or_else(|| panic!("node {} never appended the joint entry", c.nodes[f].id));
        }
        let s = admin_status(lp).unwrap();
        assert!(s.membership.is_joint() && !s.membership.committed, "{s:?}");
        c.nodes[l].kill9();

        let others: Vec<usize> = fs.iter().copied().chain([n4]).collect();
        wait_membership(&mut c, &others, &[1, 2, 3, 4], &[]);
        assert!(
            others.iter().any(|&i| c.nodes[i]
                .log_text()
                .contains("finished a joint configuration left by an earlier leader")),
            "nobody finished the joint configuration"
        );
        let nl = c.leader();
        let mut on_new = c.nodes[nl].connect();
        inserted(&on_new.put(b"after-joint"));
    }
}
