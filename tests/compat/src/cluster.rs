//! Cluster mode for server B: a 3-node `beanstalkd-rs` Raft cluster on
//! 127.0.0.1 (`insecure_plaintext`), started fresh for every case.
//!
//! # Startup
//!
//! Each node gets its own client, HTTP and cluster ports (claimed like the
//! single-server ports, see [`crate::server`]) and its own data directory;
//! every node bootstraps the cluster with `--cluster-init`. Every node gets
//! the case's `!args` (never `-b`: the Raft log replaces the binlog, and
//! `-b` with `[cluster]` is a configuration error). The cluster is ready
//! when every node answers `/readyz` with 200 and one node's `/admin`
//! reports `cluster.role == "leader"` and `cluster.ready`. The harness
//! then picks the node every client connection of the case goes to (the
//! *client node*, see [`ClusterTarget`]) and sends it exactly one
//! plaintext `quit` probe, the same single probe connection a
//! single server gets (so `total-connections` agrees). Readiness itself
//! is polled over HTTP only, which never counts as a connection.
//!
//! # `restart` / `crash`
//!
//! Both restart the **whole** cluster on the same ports and data
//! directories (without `--cluster-init`):
//!
//! - `crash`: SIGKILL every node, then drop the case's connections. The
//!   restarted client node disconnects its previous process's connections
//!   with one `DropNode` entry, in ascending connection order, which is
//!   the order the case opened them.
//! - `restart`: close the case's connections one by one in the order they
//!   were opened (like [`crate::conn::close_in_order`]), then SIGTERM every
//!   node. Closing first keeps the order deterministic: on a graceful
//!   shutdown the node closes its sockets itself, and their `Disconnect`s
//!   reach the log in the order the connection tasks end.
//!
//! Then the harness waits `downtime`, starts every node, waits until the
//! cluster is ready again, and picks the client node again (leadership may
//! have moved). No probe connection is made after a restart.
//!
//! Everything committed survives a cluster restart, including connection
//! counters, pause and drain state and reservations of connections that
//! were never disconnected. The runner therefore compares against a
//! reference server that keeps running and only sees every connection
//! close in opening order ([`crate::conn::DisconnectOnRestart`]); see
//! docs/COMPAT.md, "Cluster mode".
//!
//! # Signals
//!
//! `signal` steps go to the client node's process (SIGUSR1 on any node
//! puts the whole cluster in drain mode).

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use crate::conn::{ClientTransport, ConnHandle, Target, close_in_order};
use crate::dsl::StopMode;
use crate::server::{TempDir, claim_free_port, probe_ready, release_port};

/// Number of nodes of a harness cluster.
pub const CLUSTER_SIZE: u64 = 3;

/// How long a cluster may take to become ready (elections under several
/// parallel debug-build clusters can be slow).
const READY_TIMEOUT: Duration = Duration::from_secs(20);
/// How long a node may take to exit after SIGTERM (a graceful shutdown
/// without quorum waits up to one second for its `Disconnect`s).
const STOP_TIMEOUT: Duration = Duration::from_secs(10);
const START_ATTEMPTS: u32 = 3;
const HTTP_TIMEOUT: Duration = Duration::from_secs(2);
const POLL: Duration = Duration::from_millis(20);

/// Which node the case's client connections go to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterTarget {
    /// The leader (as of the cluster's start or last restart).
    Leader,
    /// A follower: the lowest-id follower at the cluster's start, kept
    /// after a restart unless it became the leader.
    Follower,
}

impl ClusterTarget {
    pub fn name(self) -> &'static str {
        match self {
            ClusterTarget::Leader => "leader",
            ClusterTarget::Follower => "follower",
        }
    }
}

/// Cases that cannot run against a cluster, with the reason. Each reason
/// is checked by the cluster suites: the case must fail to start with
/// `refusal` in the error (see [`check_excluded`]).
pub struct Exclusion {
    pub case: &'static str,
    /// A substring of the harness error the case must produce.
    pub refusal: &'static str,
    pub reason: &'static str,
}

/// See docs/COMPAT.md, "Cluster mode", item C3.
pub const EXCLUDED_CASES: &[Exclusion] = &[
    Exclusion {
        case: "max_job_size_clamped",
        refusal: "too large for cluster mode",
        reason: "-z 2000000000 (clamped to 1 GiB): a job must fit in one cluster frame",
    },
    Exclusion {
        case: "max_job_size_negative_wraps",
        refusal: "too large for cluster mode",
        reason: "-z -1 (wraps, clamped to 1 GiB): a job must fit in one cluster frame",
    },
    Exclusion {
        case: "max_job_size_overflow_saturates",
        refusal: "too large for cluster mode",
        reason: "-z beyond u64 (saturates, clamped to 1 GiB): a job must fit in one cluster frame",
    },
];

/// The exclusion for the case at `path`, if any.
pub fn exclusion_for(path: &Path) -> Option<&'static Exclusion> {
    let stem = path.file_stem()?.to_str()?;
    EXCLUDED_CASES.iter().find(|e| e.case == stem)
}

struct Node {
    id: u64,
    client_port: u16,
    http_port: u16,
    cluster_port: u16,
    config: PathBuf,
    log: PathBuf,
    child: Option<Child>,
}

impl Node {
    fn client_addr(&self) -> SocketAddr {
        ([127, 0, 0, 1], self.client_port).into()
    }

    fn http_addr(&self) -> SocketAddr {
        ([127, 0, 0, 1], self.http_port).into()
    }

    fn start(&mut self, bin: &Path, extra_args: &[String], init: bool) -> Result<(), String> {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log)
            .map_err(|e| format!("could not open {}: {e}", self.log.display()))?;
        let log_err = log
            .try_clone()
            .map_err(|e| format!("could not clone log handle: {e}"))?;
        let mut cmd = Command::new(bin);
        cmd.arg("--config").arg(&self.config);
        if init {
            cmd.arg("--cluster-init");
        }
        let child = cmd
            .args(extra_args)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(log_err))
            .spawn()
            .map_err(|e| format!("failed to spawn {}: {e}", bin.display()))?;
        self.child = Some(child);
        Ok(())
    }

    fn pid(&self) -> Option<u32> {
        self.child.as_ref().map(Child::id)
    }

    /// The exit status, if the node has exited (and is then reaped).
    fn exited(&mut self) -> Option<String> {
        let child = self.child.as_mut()?;
        match child.try_wait() {
            Ok(Some(status)) => {
                self.child = None;
                Some(status.to_string())
            }
            Ok(None) => None,
            Err(e) => Some(format!("wait failed: {e}")),
        }
    }

    fn kill(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }

    /// SIGTERM via kill(1) (keeps this crate free of `unsafe` and libc).
    fn term(&self) -> Result<(), String> {
        let Some(pid) = self.pid() else {
            return Ok(());
        };
        let status = Command::new("kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .status()
            .map_err(|e| format!("failed to run kill: {e}"))?;
        if status.success() {
            Ok(())
        } else {
            Err(format!("kill -TERM {pid} exited with {status}"))
        }
    }

    /// Wait for the node to exit; SIGKILL it after `timeout`.
    fn wait_exit(&mut self, timeout: Duration) -> Result<(), String> {
        let deadline = Instant::now() + timeout;
        while self.child.is_some() {
            if self.exited().is_some() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                self.kill();
                return Err(format!(
                    "node {} did not exit within {timeout:?} after SIGTERM",
                    self.id
                ));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    }

    fn log_tail(&self) -> String {
        let text = std::fs::read_to_string(&self.log).unwrap_or_default();
        let lines: Vec<&str> = text.lines().collect();
        let start = lines.len().saturating_sub(15);
        lines[start..].join("\n")
    }

    fn readyz(&self) -> bool {
        http_get(self.http_addr(), "/readyz").is_some_and(|(status, _)| status == 200)
    }

    /// `/admin`'s `cluster.role == "leader" && cluster.ready`.
    fn is_ready_leader(&self) -> bool {
        let Some((200, body)) = http_get(self.http_addr(), "/admin") else {
            return false;
        };
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&body) else {
            return false;
        };
        v["cluster"]["role"] == "leader" && v["cluster"]["ready"] == true
    }
}

/// A minimal HTTP/1.1 GET: `(status, body)`, or `None` if the request
/// failed.
fn http_get(addr: SocketAddr, path: &str) -> Option<(u16, String)> {
    let mut s = TcpStream::connect_timeout(&addr, HTTP_TIMEOUT).ok()?;
    s.set_read_timeout(Some(HTTP_TIMEOUT)).ok()?;
    s.set_write_timeout(Some(HTTP_TIMEOUT)).ok()?;
    let req = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    s.write_all(req.as_bytes()).ok()?;
    let mut resp = Vec::new();
    s.read_to_end(&mut resp).ok()?;
    let text = String::from_utf8_lossy(&resp);
    let status = text.split(' ').nth(1)?.parse().ok()?;
    let body = text.split_once("\r\n\r\n").map(|(_, b)| b.to_string())?;
    Some((status, body))
}

/// A running 3-node cluster (see the module docs). Every node is killed
/// (SIGKILL) and its ports released on drop; the directory holding the
/// configurations, logs and data directories is removed afterwards.
pub struct ClusterProcess {
    bin: PathBuf,
    extra_args: Vec<String>,
    target: ClusterTarget,
    nodes: Vec<Node>,
    /// Index of the client node in `nodes`.
    client: usize,
    // Dropped after the nodes have been killed (`Drop::drop` runs first).
    dir: TempDir,
}

impl Drop for ClusterProcess {
    fn drop(&mut self) {
        for n in &mut self.nodes {
            n.kill();
            for p in [n.client_port, n.http_port, n.cluster_port] {
                release_port(p);
            }
        }
    }
}

impl ClusterProcess {
    /// Start a fresh cluster of `bin` with `extra_args` on every node, wait
    /// until it is ready, pick the client node and probe it once. Retried
    /// with fresh ports if a node fails to start.
    pub fn spawn(
        bin: &Path,
        extra_args: &[String],
        target: ClusterTarget,
    ) -> Result<ClusterProcess, String> {
        if !bin.exists() {
            return Err(format!("server binary not found: {}", bin.display()));
        }
        let mut last_err = String::new();
        for _ in 0..START_ATTEMPTS {
            let mut cluster = Self::configure(bin, extra_args, target)?;
            match cluster.start_all(true) {
                Ok(()) => {
                    // The one probe connection a single server gets too.
                    let addr = cluster.nodes[cluster.client].client_addr();
                    if !probe_ready(addr, READY_TIMEOUT, &ClientTransport::Plain) {
                        return Err(format!(
                            "cluster probe on node {} failed\n{}",
                            cluster.nodes[cluster.client].id,
                            cluster.logs()
                        ));
                    }
                    return Ok(cluster);
                }
                Err(e) => {
                    last_err = e;
                    // A configuration error will not go away with other
                    // ports.
                    if last_err.contains("invalid configuration") {
                        break;
                    }
                }
            }
        }
        Err(format!("cluster failed to start: {last_err}"))
    }

    fn configure(
        bin: &Path,
        extra_args: &[String],
        target: ClusterTarget,
    ) -> Result<ClusterProcess, String> {
        let dir = TempDir::with_prefix("bstk-compat-cluster")
            .map_err(|e| format!("could not create cluster directory: {e}"))?;
        let mut c = ClusterProcess {
            bin: bin.to_path_buf(),
            extra_args: extra_args.to_vec(),
            target,
            nodes: Vec::new(),
            client: 0,
            dir,
        };
        for id in 1..=CLUSTER_SIZE {
            // Pushed at once so that `Drop` releases every claimed port.
            let mut ports = [0u16; 3];
            for p in &mut ports {
                *p = claim_free_port().map_err(|e| format!("no free port: {e}"))?;
            }
            c.nodes.push(Node {
                id,
                client_port: ports[0],
                http_port: ports[1],
                cluster_port: ports[2],
                config: c.dir.path().join(format!("node{id}.toml")),
                log: c.dir.path().join(format!("node{id}.log")),
                child: None,
            });
        }
        let peers: String = c
            .nodes
            .iter()
            .map(|n| {
                format!(
                    "[[cluster.peer]]\nid = {}\naddr = \"127.0.0.1:{}\"\n",
                    n.id, n.cluster_port
                )
            })
            .collect();
        for n in &c.nodes {
            let data = c.dir.path().join(format!("data{}", n.id));
            let text = format!(
                "# Generated by the bstk-compat harness (cluster mode).\n\
                 [[listener]]\naddr = \"127.0.0.1:{}\"\n\
                 [http]\naddr = \"127.0.0.1:{}\"\n\
                 [cluster]\nnode_id = {}\nlisten = \"127.0.0.1:{}\"\n\
                 data_dir = {}\ninsecure_plaintext = true\n{peers}",
                n.client_port,
                n.http_port,
                n.id,
                n.cluster_port,
                crate::server::toml_string(&data),
            );
            std::fs::write(&n.config, text)
                .map_err(|e| format!("could not write {}: {e}", n.config.display()))?;
        }
        Ok(c)
    }

    /// The last lines of every node's log, for error messages.
    fn logs(&self) -> String {
        self.nodes
            .iter()
            .map(|n| format!("--- node {} log ---\n{}", n.id, n.log_tail()))
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Start every node (with `--cluster-init` if `init`), wait
    /// until the cluster is ready and pick the client node.
    fn start_all(&mut self, init: bool) -> Result<(), String> {
        for n in &mut self.nodes {
            n.start(&self.bin, &self.extra_args, init)?;
        }
        let deadline = Instant::now() + READY_TIMEOUT;
        let mut ready = vec![false; self.nodes.len()];
        loop {
            for (i, n) in self.nodes.iter_mut().enumerate() {
                if let Some(status) = n.exited() {
                    let err = format!("node {} exited early with {status}", n.id);
                    let logs = self.logs();
                    self.stop_all(StopMode::Kill)?;
                    return Err(format!("{err}\n{logs}"));
                }
                if !ready[i] {
                    ready[i] = n.readyz();
                }
            }
            if ready.iter().all(|&r| r)
                && let Some(leader) = self.nodes.iter().position(Node::is_ready_leader)
            {
                self.pick_client(leader);
                return Ok(());
            }
            if Instant::now() >= deadline {
                let logs = self.logs();
                self.stop_all(StopMode::Kill)?;
                return Err(format!(
                    "cluster not ready within {READY_TIMEOUT:?} (ready: {ready:?})\n{logs}"
                ));
            }
            std::thread::sleep(POLL);
        }
    }

    fn pick_client(&mut self, leader: usize) {
        self.client = match self.target {
            ClusterTarget::Leader => leader,
            // Keep the current follower (initially index 0, which is
            // replaced below if it leads) unless it became the leader.
            ClusterTarget::Follower if self.client != leader => self.client,
            ClusterTarget::Follower => (0..self.nodes.len())
                .find(|&i| i != leader)
                .expect("a cluster has at least one follower"),
        };
    }

    /// Stop every node: SIGKILL all at once, or SIGTERM all at once and
    /// wait for each to exit.
    fn stop_all(&mut self, how: StopMode) -> Result<(), String> {
        match how {
            StopMode::Kill => {
                for n in &mut self.nodes {
                    n.kill();
                }
                Ok(())
            }
            StopMode::Term => {
                let mut first_err = None;
                for n in &self.nodes {
                    if let Err(e) = n.term() {
                        first_err.get_or_insert(e);
                    }
                }
                for n in &mut self.nodes {
                    if let Err(e) = n.wait_exit(STOP_TIMEOUT) {
                        first_err.get_or_insert(e);
                    }
                }
                first_err.map_or(Ok(()), Err)
            }
        }
    }

    /// The node the case's connections go to (1-based id).
    pub fn client_node_id(&self) -> u64 {
        self.nodes[self.client].id
    }
}

impl Target for ClusterProcess {
    fn addr(&self) -> SocketAddr {
        self.nodes[self.client].client_addr()
    }

    fn client_transport(&self) -> ClientTransport {
        ClientTransport::Plain
    }

    fn pid(&self) -> u32 {
        // A node that is not running has no pid; `kill 0` would signal
        // the harness's process group, so use an impossible pid instead.
        self.nodes[self.client].pid().unwrap_or(u32::MAX)
    }

    fn restart(
        &mut self,
        how: StopMode,
        downtime: Duration,
        open: Vec<ConnHandle>,
    ) -> Result<(), String> {
        match how {
            StopMode::Kill => {
                self.stop_all(StopMode::Kill)?;
                drop(open);
            }
            StopMode::Term => {
                close_in_order(open);
                self.stop_all(StopMode::Term)?;
            }
        }
        std::thread::sleep(downtime);
        let mut last_err = String::new();
        for _ in 0..START_ATTEMPTS {
            match self.start_all(false) {
                Ok(()) => return Ok(()),
                Err(e) => last_err = e,
            }
        }
        Err(format!("cluster failed to restart: {last_err}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conn::{Outcome, connect};

    fn rs_bin() -> PathBuf {
        let bin = crate::runner::default_rs_bin();
        assert!(
            bin.exists(),
            "beanstalkd-rs binary missing: {}",
            bin.display()
        );
        bin
    }

    fn leader_index(c: &ClusterProcess) -> Option<usize> {
        c.nodes.iter().position(Node::is_ready_leader)
    }

    fn roundtrip(c: &ClusterProcess, cmd: &[u8]) -> Vec<u8> {
        let mut conn =
            connect(c.addr(), &ClientTransport::Plain, Duration::from_secs(2)).expect("connect");
        conn.send(cmd).expect("send");
        match conn.recv_response() {
            Outcome::Received(r) => r,
            other => panic!("no reply to {cmd:?}: {other:?}"),
        }
    }

    /// The client node is the leader / a follower as requested, and both
    /// kinds of restart really restart every node on the same data (a
    /// committed job survives) and ports.
    #[test]
    fn cluster_targets_and_full_restarts() {
        let bin = rs_bin();
        for target in [ClusterTarget::Leader, ClusterTarget::Follower] {
            let mut c = ClusterProcess::spawn(&bin, &[], target).expect("spawn cluster");
            let leader = leader_index(&c).expect("leader");
            assert_eq!(c.client == leader, target == ClusterTarget::Leader);
            assert_eq!(roundtrip(&c, b"put 0 0 60 1\r\nx\r\n"), b"INSERTED 1\r\n");
            let ports: Vec<u16> = c.nodes.iter().map(|n| n.client_port).collect();
            for how in [StopMode::Term, StopMode::Kill] {
                let pids: Vec<Option<u32>> = c.nodes.iter().map(Node::pid).collect();
                Target::restart(&mut c, how, Duration::ZERO, Vec::new()).expect("restart");
                for (n, old) in c.nodes.iter().zip(&pids) {
                    assert!(
                        n.pid().is_some() && n.pid() != *old,
                        "node {} not restarted",
                        n.id
                    );
                }
                let leader = leader_index(&c).expect("leader after restart");
                assert_eq!(c.client == leader, target == ClusterTarget::Leader);
                assert_eq!(roundtrip(&c, b"peek 1\r\n"), b"FOUND 1 1\r\nx\r\n");
            }
            let after: Vec<u16> = c.nodes.iter().map(|n| n.client_port).collect();
            assert_eq!(ports, after);
        }
    }

    #[test]
    fn every_exclusion_names_an_existing_case() {
        for e in EXCLUDED_CASES {
            let path = crate::runner::cases_dir().join(format!("{}.bt", e.case));
            assert!(path.is_file(), "{}", path.display());
            assert_eq!(exclusion_for(&path).map(|x| x.case), Some(e.case));
        }
    }
}
