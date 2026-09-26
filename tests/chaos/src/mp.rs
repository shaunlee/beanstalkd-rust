//! Multi-process chaos harness: N real `beanstalkd-rs` processes in cluster
//! mode (`insecure_plaintext`), every directed cluster link through a
//! [`Proxy`], a seeded random fault schedule, a client workload recorded
//! for the history checker, then heal and verify.
//!
//! Each node's `[[cluster.peer]]` address for another node is the proxy of
//! that directed link (its own entry is its real cluster port), so Raft
//! RPCs, forwards and control requests from A to B all cross the proxy
//! `A → B` (checked at startup: every link must carry traffic).
//!
//! Faults: kill -9 (a node, the leader, all), SIGSTOP / SIGCONT (a node or
//! the leader), proxy partitions (isolate a node, cut a pair, one-way cut,
//! stall either direction of a link), added latency, and wiping a node's
//! data directory before a restart (off with `BSTK_CHAOS_NO_WIPE`). Every
//! initial node starts with `--cluster-init`; a wiped node restarts without
//! it and rejoins in the server's rejoin mode. A wipe is skipped when it
//! would leave fewer nodes with their data than a quorum. Clock skew is not
//! injected: the server anchors its engine clock to `SystemTime` at start
//! with no way to offset it.
//!
//! After the schedule: heal every link, SIGCONT and restart every node,
//! wait until all are ready, let the workload finish, wait for every TTR
//! and `DropNode`, then verify (`peek` / `stats-job` every job ever seen,
//! kick everything, drain), check that every node's replicated counters
//! agree, scan the node logs for panics, and run the history checker.
//!
//! Processes are tracked and killed (SIGKILL, which also ends stopped
//! processes) when the run ends or fails, including on panic (`Drop`).

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use bstk_raft::sim::SimRng;
use nix::sys::signal::Signal;
use serde_json::Value;

use crate::checker::{self, CheckConfig, Report};
use crate::client::{BsClient, http_get};
use crate::history::{Cmd, ConnKey, History, JobId, Recorder, Reply};
use crate::proxy::Proxy;
use crate::workload::{ClientState, Known, WorkloadConfig};

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// The `beanstalkd-rs` binary: `BSTK_RS_BIN`, else next to the test
/// executable (`target/<profile>/deps/..`), else `CARGO_TARGET_DIR/debug`
/// or the repository's `target/debug`.
pub fn server_bin() -> PathBuf {
    if let Ok(p) = std::env::var("BSTK_RS_BIN") {
        return PathBuf::from(p);
    }
    if let Ok(exe) = std::env::current_exe()
        && let Some(profile_dir) = exe.parent().and_then(Path::parent)
    {
        let p = profile_dir.join("beanstalkd-rs");
        if p.exists() {
            return p;
        }
    }
    let target = std::env::var("CARGO_TARGET_DIR").map_or_else(
        |_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target"),
        PathBuf::from,
    );
    target.join("debug").join("beanstalkd-rs")
}

#[derive(Debug, Clone)]
pub struct MpConfig {
    pub seed: u64,
    pub nodes: u64,
    pub clients: usize,
    /// Duration of the fault schedule (the workload runs throughout).
    pub duration: Duration,
    pub node_timeout: &'static str,
    pub snapshot_every: u64,
    pub wipe: bool,
    pub work: WorkloadConfig,
    /// Where run directories go (a temporary directory if `None`); failed
    /// runs keep theirs.
    pub base_dir: Option<PathBuf>,
}

impl MpConfig {
    pub fn from_seed(seed: u64, duration: Duration) -> MpConfig {
        let mut r = SimRng::new(seed ^ 0x3217_AB00);
        MpConfig {
            seed,
            nodes: 3,
            clients: r.range(4, 7) as usize,
            duration,
            node_timeout: "1s",
            snapshot_every: 150,
            wipe: std::env::var("BSTK_CHAOS_NO_WIPE").is_err(),
            work: WorkloadConfig {
                max_puts: 150,
                ..WorkloadConfig::default()
            },
            base_dir: std::env::var("BSTK_CHAOS_DIR").ok().map(PathBuf::from),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum MpFault {
    Kill(u64),
    KillLeader,
    KillAll,
    Restart(u64),
    RestartAll,
    Stop(u64),
    StopLeader,
    Cont(u64),
    /// Sever every link to and from the node.
    Isolate(u64),
    IsolateLeader,
    /// Sever both directions between two nodes.
    Cut(u64, u64),
    /// Sever one directed link.
    OneWay(u64, u64),
    /// Stall a directed link (up = requests, down = responses).
    Stall(u64, u64, bool, bool),
    /// Latency on every link (ms).
    Latency(u64),
    /// Heal every link (`true`: reset the connections).
    Heal(bool),
    Wipe(u64),
}

pub fn generate(seed: u64, nodes: u64, duration: Duration, wipe: bool) -> Vec<(Duration, MpFault)> {
    let mut r = SimRng::new(seed ^ 0x0FA1_7000);
    let mut out = Vec::new();
    let mut at = Duration::from_millis(r.range(500, 2000));
    let pick = |r: &mut SimRng| r.range(1, nodes);
    while at < duration {
        let f = match r.range(0, 21) {
            0 => MpFault::Kill(pick(&mut r)),
            1 | 2 => MpFault::KillLeader,
            3 => MpFault::KillAll,
            4 | 5 => MpFault::Restart(pick(&mut r)),
            6 => MpFault::RestartAll,
            7 => MpFault::Stop(pick(&mut r)),
            8 => MpFault::StopLeader,
            9 => MpFault::Cont(pick(&mut r)),
            10 => MpFault::Isolate(pick(&mut r)),
            11 => MpFault::IsolateLeader,
            12 => {
                let a = pick(&mut r);
                MpFault::Cut(a, a % nodes + 1)
            }
            13 => {
                let a = pick(&mut r);
                MpFault::OneWay(a, a % nodes + 1)
            }
            14 => {
                let a = pick(&mut r);
                let up = r.range(0, 1) == 0;
                MpFault::Stall(a, a % nodes + 1, up, !up || r.range(0, 1) == 0)
            }
            15 => MpFault::Latency(r.range(1, 40)),
            16 | 20 if wipe => MpFault::Wipe(pick(&mut r)),
            16..=19 => MpFault::Heal(r.range(0, 1) == 0),
            _ => MpFault::Cont(pick(&mut r)),
        };
        out.push((at, f));
        at += Duration::from_millis(r.range(700, 3500));
    }
    out
}

struct Node {
    id: u64,
    client: SocketAddr,
    http: SocketAddr,
    cluster: SocketAddr,
    data_dir: PathBuf,
    config: PathBuf,
    log: PathBuf,
    child: Option<Child>,
    stopped: bool,
    starts: u32,
}

impl Node {
    fn start(&mut self, bin: &Path, init: bool) -> Result<(), String> {
        if self.child.is_some() {
            return Ok(());
        }
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log)
            .map_err(|e| format!("log {}: {e}", self.log.display()))?;
        let mut cmd = Command::new(bin);
        cmd.arg("--config").arg(&self.config);
        if init {
            cmd.arg("--cluster-init");
        }
        let child = cmd
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .map_err(|e| format!("spawn {}: {e}", bin.display()))?;
        self.child = Some(child);
        self.stopped = false;
        self.starts += 1;
        Ok(())
    }

    fn pid(&self) -> Option<nix::unistd::Pid> {
        let c = self.child.as_ref()?;
        Some(nix::unistd::Pid::from_raw(i32::try_from(c.id()).ok()?))
    }

    fn signal(&mut self, sig: Signal) {
        if let Some(pid) = self.pid() {
            let _ = nix::sys::signal::kill(pid, sig);
            match sig {
                Signal::SIGSTOP => self.stopped = true,
                Signal::SIGCONT => self.stopped = false,
                _ => {}
            }
        }
    }

    fn kill9(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
        self.stopped = false;
    }

    /// Whether the process exited on its own (reaps it).
    fn exited(&mut self) -> Option<std::process::ExitStatus> {
        let st = self.child.as_mut()?.try_wait().ok()??;
        self.child = None;
        Some(st)
    }
}

/// A running cluster; `Drop` kills every process.
struct Cluster {
    bin: PathBuf,
    dir: PathBuf,
    _tmp: Option<tempfile::TempDir>,
    nodes: Vec<Node>,
    proxies: BTreeMap<(u64, u64), Proxy>,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for n in &mut self.nodes {
            n.kill9();
        }
    }
}

fn free_addr() -> Result<SocketAddr, String> {
    let l = std::net::TcpListener::bind(("127.0.0.1", 0)).map_err(|e| format!("bind: {e}"))?;
    l.local_addr().map_err(|e| format!("local_addr: {e}"))
}

impl Cluster {
    async fn create(cfg: &MpConfig, run: u64) -> Result<Cluster, String> {
        let bin = server_bin();
        if !bin.exists() {
            return Err(format!(
                "{} not found: build it first (cargo build -p bstk-server) or set BSTK_RS_BIN",
                bin.display()
            ));
        }
        let (tmp, dir) = match &cfg.base_dir {
            Some(b) => {
                let d = b.join(format!("run-{}-{run}", cfg.seed));
                if d.exists() {
                    std::fs::remove_dir_all(&d).map_err(|e| e.to_string())?;
                }
                std::fs::create_dir_all(&d).map_err(|e| e.to_string())?;
                (None, d)
            }
            None => {
                let t = tempfile::Builder::new()
                    .prefix("bstk-chaos-mp-")
                    .tempdir()
                    .map_err(|e| e.to_string())?;
                let d = t.path().to_path_buf();
                (Some(t), d)
            }
        };
        let mut nodes = Vec::new();
        for id in 1..=cfg.nodes {
            nodes.push(Node {
                id,
                client: free_addr()?,
                http: free_addr()?,
                cluster: free_addr()?,
                data_dir: dir.join(format!("data{id}")),
                config: dir.join(format!("node{id}.toml")),
                log: dir.join(format!("node{id}.log")),
                child: None,
                stopped: false,
                starts: 0,
            });
        }
        let mut proxies = BTreeMap::new();
        for a in 1..=cfg.nodes {
            for b in 1..=cfg.nodes {
                if a != b {
                    let target = nodes[(b - 1) as usize].cluster;
                    let p = Proxy::start(target)
                        .await
                        .map_err(|e| format!("proxy {a}->{b}: {e}"))?;
                    proxies.insert((a, b), p);
                }
            }
        }
        for n in &nodes {
            let mut peers = String::new();
            for m in &nodes {
                let addr = if m.id == n.id {
                    m.cluster
                } else {
                    proxies
                        .get(&(n.id, m.id))
                        .map(Proxy::addr)
                        .ok_or("missing proxy")?
                };
                peers.push_str(&format!(
                    "[[cluster.peer]]\nid = {}\naddr = \"{addr}\"\n",
                    m.id
                ));
            }
            let text = format!(
                "[[listener]]\naddr = \"{}\"\n\
                 [http]\naddr = \"{}\"\nsnapshot_min_interval = \"0s\"\n\
                 [cluster]\nnode_id = {}\nlisten = \"{}\"\ndata_dir = \"{}\"\n\
                 node_timeout = \"{}\"\nsnapshot_every = {}\ninsecure_plaintext = true\n\n{peers}",
                n.client,
                n.http,
                n.id,
                n.cluster,
                n.data_dir.display(),
                cfg.node_timeout,
                cfg.snapshot_every,
            );
            std::fs::write(&n.config, text).map_err(|e| e.to_string())?;
        }
        Ok(Cluster {
            bin,
            dir,
            _tmp: tmp,
            nodes,
            proxies,
        })
    }

    fn node(&mut self, id: u64) -> Option<&mut Node> {
        self.nodes.get_mut((id - 1) as usize)
    }

    async fn admin(&self, id: u64) -> Option<Value> {
        let n = self.nodes.get((id - 1) as usize)?;
        let (st, body) = http_get(n.http, "/admin", Duration::from_millis(500)).await?;
        if st != 200 {
            return None;
        }
        serde_json::from_str(&body).ok()
    }

    async fn ready(&self, id: u64) -> bool {
        let Some(n) = self.nodes.get((id - 1) as usize) else {
            return false;
        };
        http_get(n.http, "/readyz", Duration::from_millis(500))
            .await
            .is_some_and(|(s, _)| s == 200)
    }

    /// The ready leader, per `/admin`.
    async fn leader(&self) -> Option<u64> {
        for n in &self.nodes {
            if n.child.is_none() || n.stopped {
                continue;
            }
            if let Some(a) = self.admin(n.id).await
                && a["cluster"]["role"] == "leader"
            {
                return Some(n.id);
            }
        }
        None
    }

    fn links_of(&self, id: u64) -> Vec<&Proxy> {
        self.proxies
            .iter()
            .filter(|((a, b), _)| *a == id || *b == id)
            .map(|(_, p)| p)
            .collect()
    }

    fn log_tail(&self, id: u64, lines: usize) -> String {
        let Some(n) = self.nodes.get((id - 1) as usize) else {
            return String::new();
        };
        let text = std::fs::read_to_string(&n.log).unwrap_or_default();
        let v: Vec<&str> = text.lines().collect();
        v[v.len().saturating_sub(lines)..].join("\n")
    }
}

/// The result of one multi-process run.
#[derive(Debug)]
pub struct MpOutcome {
    pub cfg: MpConfig,
    pub failures: Vec<String>,
    pub report: Report,
    pub events: Vec<String>,
    pub history: History,
    pub dir: Option<PathBuf>,
    pub wall: Duration,
    /// Faults applied, by kind.
    pub faults: BTreeMap<String, u64>,
}

impl MpOutcome {
    pub fn passed(&self) -> bool {
        self.failures.is_empty()
    }

    pub fn describe(&self) -> String {
        let mut s = format!(
            "mp seed {} ({} nodes, {} clients, {:?} faults{}): {} in {:.1?} \
             [replay: BSTK_CHAOS_MP_SEED={} cargo test -p bstk-chaos --test multiprocess replay \
             -- --ignored --nocapture]\n",
            self.cfg.seed,
            self.cfg.nodes,
            self.cfg.clients,
            self.cfg.duration,
            if self.cfg.wipe { ", wipes" } else { "" },
            if self.passed() { "ok" } else { "FAILED" },
            self.wall,
            self.cfg.seed,
        );
        for f in &self.failures {
            s.push_str(&format!("  failure: {f}\n"));
        }
        if !self.passed() {
            if let Some(d) = &self.dir {
                s.push_str(&format!("  run directory (logs): {}\n", d.display()));
            }
            s.push_str("  events:\n");
            for e in &self.events {
                s.push_str(&format!("    {e}\n"));
            }
        }
        s
    }
}

struct Shared {
    t0: Instant,
    events: Mutex<Vec<String>>,
    problems: Mutex<Vec<String>>,
    faults: Mutex<BTreeMap<String, u64>>,
}

impl Shared {
    fn elapsed(&self) -> Duration {
        self.t0.elapsed()
    }

    fn event(&self, e: String) {
        let t = self.elapsed();
        lock(&self.events).push(format!("{t:.3?} {e}"));
    }

    fn problem(&self, p: String) {
        lock(&self.problems).push(p);
    }

    fn count(&self, kind: &str) {
        *lock(&self.faults).entry(kind.to_string()).or_default() += 1;
    }
}

/// Runs one multi-process chaos run (on a multi-thread tokio runtime).
pub async fn run(cfg: MpConfig) -> MpOutcome {
    let t0 = Instant::now();
    let sh = Arc::new(Shared {
        t0,
        events: Mutex::new(Vec::new()),
        problems: Mutex::new(Vec::new()),
        faults: Mutex::new(BTreeMap::new()),
    });
    let rec = Recorder::new();
    let mut dir = None;
    let mut attempt = 0;
    let cluster = loop {
        attempt += 1;
        match start_cluster(&cfg, attempt, &sh).await {
            Ok(c) => break Some(c),
            Err(e) if attempt < 3 => {
                sh.event(format!("cluster start failed ({e}); retrying"));
            }
            Err(e) => {
                sh.problem(format!("cluster start: {e}"));
                break None;
            }
        }
    };
    if let Some(mut c) = cluster {
        dir = Some(c.dir.clone());
        drive(&mut c, &cfg, &sh, &rec).await;
        // Panics anywhere in the nodes' logs.
        for n in &c.nodes {
            let text = std::fs::read_to_string(&n.log).unwrap_or_default();
            if let Some(line) = text.lines().find(|l| l.contains("panicked")) {
                sh.problem(format!("node {} panicked: {line}", n.id));
            }
        }
        let keep = !lock(&sh.problems).is_empty();
        drop(c);
        if !keep
            && cfg.base_dir.is_some()
            && let Some(d) = &dir
        {
            let _ = std::fs::remove_dir_all(d);
        }
    }
    let history = rec.history();
    let report = checker::check(
        &history,
        &CheckConfig {
            slack: Duration::from_millis(200),
            ..CheckConfig::default()
        },
    );
    let mut failures = lock(&sh.problems).clone();
    failures.extend(report.violations.iter().map(|v| v.to_string()));
    let events = lock(&sh.events).clone();
    let faults = lock(&sh.faults).clone();
    MpOutcome {
        cfg,
        failures,
        report,
        events,
        history,
        dir: if cfg_keeps(&dir) { dir } else { None },
        wall: t0.elapsed(),
        faults,
    }
}

fn cfg_keeps(dir: &Option<PathBuf>) -> bool {
    dir.as_ref().is_some_and(|d| d.exists())
}

async fn wait_for<F, Fut>(within: Duration, mut f: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + within;
    loop {
        if f().await {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn start_cluster(cfg: &MpConfig, attempt: u64, sh: &Shared) -> Result<Cluster, String> {
    let c = Cluster::create(cfg, attempt).await?;
    let dir = c.dir.clone();
    let res = start_nodes(c, sh).await;
    if res.is_err() && cfg.base_dir.is_some() {
        // Keep only a failed run's logs, not a failed start's.
        let _ = std::fs::remove_dir_all(&dir);
    }
    res
}

async fn start_nodes(mut c: Cluster, sh: &Shared) -> Result<Cluster, String> {
    let bin = c.bin.clone();
    // Every initial node bootstraps with `--cluster-init`.
    for i in 0..c.nodes.len() {
        c.nodes[i].start(&bin, true)?;
    }
    let ids: Vec<u64> = c.nodes.iter().map(|n| n.id).collect();
    let cref = &c;
    let ok = wait_for(Duration::from_secs(30), || async {
        for &id in &ids {
            if !cref.ready(id).await {
                return false;
            }
        }
        true
    })
    .await;
    if !ok {
        let mut tails = String::new();
        for &id in &ids {
            tails.push_str(&format!("\n--- node {id} ---\n{}", c.log_tail(id, 15)));
        }
        return Err(format!("the cluster did not become ready:{tails}"));
    }
    // The leader's links carry traffic: the nodes use the proxies.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let Some(l) = c.leader().await else {
        return Err("no leader after startup".into());
    };
    for ((a, b), p) in &c.proxies {
        let (up, down) = p.bytes();
        if *a == l && (up == 0 || down == 0) {
            return Err(format!(
                "link {a} -> {b} of leader {l} carried no traffic through its proxy \
                 (up {up}, down {down})"
            ));
        }
    }
    sh.event(format!(
        "cluster ready, leader {l}; its links carry traffic through the proxies"
    ));
    Ok(c)
}

async fn apply(c: &mut Cluster, f: &MpFault, sh: &Shared) {
    let bin = c.bin.clone();
    let leader = match f {
        MpFault::KillLeader | MpFault::StopLeader | MpFault::IsolateLeader => c.leader().await,
        _ => None,
    };
    let kind = format!("{f:?}")
        .split(['(', ' '])
        .next()
        .unwrap_or_default()
        .to_string();
    match f {
        MpFault::Kill(id) => kill(c, *id, sh),
        MpFault::KillLeader => {
            if let Some(l) = leader {
                kill(c, l, sh);
            }
        }
        MpFault::KillAll => {
            for id in 1..=c.nodes.len() as u64 {
                kill(c, id, sh);
            }
        }
        MpFault::Restart(id) => restart(c, &bin, *id, false, sh),
        MpFault::RestartAll => {
            for id in 1..=c.nodes.len() as u64 {
                restart(c, &bin, id, false, sh);
            }
        }
        MpFault::Wipe(id) => {
            // Never more rejoining nodes than a majority can spare: the
            // nodes with their data must still form a quorum.
            let n = c.nodes.len();
            let spare = n - (n / 2 + 1);
            let others = c
                .nodes
                .iter()
                .filter(|m| m.id != *id && rejoin_pending(&m.data_dir))
                .count();
            if others + 1 > spare {
                sh.event(format!("wipe of node {id} skipped (others rejoining)"));
                sh.count("WipeSkipped");
                return;
            }
            kill(c, *id, sh);
            restart(c, &bin, *id, true, sh);
        }
        MpFault::Stop(id) => stop(c, *id, sh),
        MpFault::StopLeader => {
            if let Some(l) = leader {
                stop(c, l, sh);
            }
        }
        MpFault::Cont(id) => {
            if let Some(n) = c.node(*id)
                && n.stopped
            {
                n.signal(Signal::SIGCONT);
                sh.event(format!("SIGCONT node {id}"));
            }
        }
        MpFault::Isolate(id) => isolate(c, *id, sh),
        MpFault::IsolateLeader => {
            if let Some(l) = leader {
                isolate(c, l, sh);
            }
        }
        MpFault::Cut(a, b) => {
            for k in [(*a, *b), (*b, *a)] {
                if let Some(p) = c.proxies.get(&k) {
                    p.sever();
                }
            }
            sh.event(format!("cut {a} <-> {b}"));
        }
        MpFault::OneWay(a, b) => {
            if let Some(p) = c.proxies.get(&(*a, *b)) {
                p.sever();
            }
            sh.event(format!("cut {a} -> {b} (one way)"));
        }
        MpFault::Stall(a, b, up, down) => {
            if let Some(p) = c.proxies.get(&(*a, *b)) {
                p.stall(*up, *down);
            }
            sh.event(format!(
                "stall {a} -> {b} (requests {up}, responses {down})"
            ));
        }
        MpFault::Latency(ms) => {
            for p in c.proxies.values() {
                p.set_latency(Duration::from_millis(*ms));
            }
            sh.event(format!("latency {ms} ms on every link"));
        }
        MpFault::Heal(reset) => {
            for p in c.proxies.values() {
                p.heal(*reset);
            }
            sh.event(format!("heal links (reset {reset})"));
        }
    }
    sh.count(&kind);
}

fn kill(c: &mut Cluster, id: u64, sh: &Shared) {
    if let Some(n) = c.node(id)
        && n.child.is_some()
    {
        n.kill9();
        sh.event(format!("kill -9 node {id}"));
    }
}

fn stop(c: &mut Cluster, id: u64, sh: &Shared) {
    if let Some(n) = c.node(id)
        && n.child.is_some()
        && !n.stopped
    {
        n.signal(Signal::SIGSTOP);
        sh.event(format!("SIGSTOP node {id}"));
    }
}

fn isolate(c: &mut Cluster, id: u64, sh: &Shared) {
    for p in c.links_of(id) {
        p.sever();
    }
    sh.event(format!("isolate node {id}"));
}

/// Whether a node has not finished rejoining: its data directory was
/// wiped (the restarted server has not recreated its log yet) or holds the
/// server's rejoin marker.
fn rejoin_pending(data_dir: &Path) -> bool {
    !data_dir.join("log").exists() || data_dir.join("rejoin").exists()
}

fn restart(c: &mut Cluster, bin: &Path, id: u64, wipe: bool, sh: &Shared) {
    let Some(n) = c.node(id) else { return };
    if let Some(st) = n.exited() {
        sh.problem(format!("node {id} exited on its own: {st}"));
    }
    if n.child.is_some() {
        return;
    }
    if wipe {
        let _ = std::fs::remove_dir_all(&n.data_dir);
    }
    match n.start(bin, false) {
        Ok(()) => sh.event(format!(
            "{} node {id}",
            if wipe { "wipe and restart" } else { "restart" }
        )),
        Err(e) => sh.problem(e),
    }
}

/// Op timeout: longer than any reserve timeout plus a node timeout.
fn op_timeout(cmd: &Cmd) -> Duration {
    match cmd {
        Cmd::ReserveWithTimeout(t) => Duration::from_secs(u64::from(*t) + 5),
        Cmd::Reserve => Duration::from_secs(8),
        _ => Duration::from_secs(5),
    }
}

#[allow(clippy::too_many_arguments)]
async fn workload_client(
    addrs: Vec<SocketAddr>,
    rec: Recorder,
    known: Arc<Mutex<Known>>,
    work: WorkloadConfig,
    sh: Arc<Shared>,
    seed: u64,
    idx: u64,
    next_conn: Arc<Mutex<ConnKey>>,
    until: Instant,
) {
    let mut r = SimRng::new(seed ^ (idx + 7).wrapping_mul(0x9E37_79B9_7F4A));
    while Instant::now() < until {
        let addr = addrs[r.range(0, addrs.len() as u64 - 1) as usize];
        let Ok(mut cl) = BsClient::connect(addr, Duration::from_secs(1)).await else {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        };
        let key = {
            let mut k = lock(&next_conn);
            *k += 1;
            *k
        };
        rec.open_conn(key, sh.elapsed());
        let mut st = ClientState::new(seed, key);
        let mut answered = 0;
        loop {
            if Instant::now() >= until {
                break;
            }
            let cmd = st.next(&mut r, &work, &mut lock(&known));
            let op = rec.begin(key, cmd.clone(), sh.elapsed());
            match tokio::time::timeout(op_timeout(&cmd), cl.call(&cmd)).await {
                Ok(Ok(reply)) => {
                    answered += 1;
                    rec.finish(op, sh.elapsed(), reply.clone());
                    st.observe(&cmd, &reply, &mut lock(&known));
                }
                _ => break,
            }
            if r.range(0, 29) == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(r.range(0, 120))).await;
        }
        drop(cl);
        rec.close_conn(key, sh.elapsed());
        if answered == 0 {
            // The node closed the connection at once (isolated, shutting
            // down): back off instead of storming it with connections.
            tokio::time::sleep(Duration::from_millis(r.range(100, 400))).await;
        }
    }
}

async fn drive(c: &mut Cluster, cfg: &MpConfig, sh: &Arc<Shared>, rec: &Recorder) {
    let start = Instant::now();
    let until = start + cfg.duration;
    let known: Arc<Mutex<Known>> = Arc::default();
    let next_conn: Arc<Mutex<ConnKey>> = Arc::default();
    let addrs: Vec<SocketAddr> = c.nodes.iter().map(|n| n.client).collect();
    let mut clients = Vec::new();
    for i in 0..cfg.clients {
        clients.push(tokio::spawn(workload_client(
            addrs.clone(),
            rec.clone(),
            known.clone(),
            cfg.work.clone(),
            sh.clone(),
            cfg.seed,
            i as u64,
            next_conn.clone(),
            until,
        )));
    }
    for (at, f) in generate(cfg.seed, cfg.nodes, cfg.duration, cfg.wipe) {
        tokio::time::sleep_until((start + at).into()).await;
        apply(c, &f, sh).await;
    }
    tokio::time::sleep_until(until.into()).await;

    // Heal everything.
    sh.event("heal: links, SIGCONT, restart".into());
    for p in c.proxies.values() {
        p.heal(false);
    }
    let bin = c.bin.clone();
    for id in 1..=c.nodes.len() as u64 {
        if let Some(n) = c.node(id)
            && n.stopped
        {
            n.signal(Signal::SIGCONT);
        }
        restart(c, &bin, id, false, sh);
    }
    let ids: Vec<u64> = c.nodes.iter().map(|n| n.id).collect();
    let cref = &*c;
    let ready = wait_for(Duration::from_secs(60), || async {
        for &id in &ids {
            if !cref.ready(id).await {
                return false;
            }
        }
        true
    })
    .await;
    if !ready {
        let mut s = String::from("liveness: not every node ready 60 s after healing:");
        for &id in &ids {
            let a = c.admin(id).await;
            s.push_str(&format!(
                "\n  node {id}: {}\n  log tail:\n{}",
                a.map_or("no /admin".into(), |a| a["cluster"].to_string()),
                c.log_tail(id, 10)
            ));
        }
        sh.problem(s);
        for cl in clients {
            cl.abort();
        }
        return;
    }
    sh.event("all nodes ready".into());
    for cl in clients {
        let _ = cl.await;
    }
    // Every TTR, every close and DropNode has taken effect.
    tokio::time::sleep(Duration::from_secs(u64::from(cfg.work.ttr.1) + 3)).await;
    verify(c, sh, rec).await;
    compare_replicas(c, sh).await;
}

async fn verify(c: &Cluster, sh: &Shared, rec: &Recorder) {
    let h = rec.history();
    let mut ids: BTreeSet<JobId> = BTreeSet::new();
    for o in &h.ops {
        if let Some(
            Reply::Inserted(id)
            | Reply::BuriedId(id)
            | Reply::Reserved { id, .. }
            | Reply::Found { id, .. },
        ) = o.acked()
        {
            ids.insert(*id);
        }
    }
    let Some(l) = c.leader().await else {
        sh.problem("verification: no leader".into());
        return;
    };
    let addr = c.nodes[(l - 1) as usize].client;
    let Ok(mut cl) = BsClient::connect(addr, Duration::from_secs(2)).await else {
        sh.problem("verification: cannot connect".into());
        return;
    };
    let key: ConnKey = 1 << 40;
    rec.open_conn(key, sh.elapsed());
    let mut cmds = Vec::new();
    for &id in &ids {
        cmds.push(Cmd::Peek(id));
        cmds.push(Cmd::StatsJob(id));
    }
    cmds.push(Cmd::Kick(1_000_000));
    let mut ok = true;
    let call = async |cl: &mut BsClient, cmd: Cmd| -> Option<Reply> {
        let op = rec.begin(key, cmd.clone(), sh.elapsed());
        match tokio::time::timeout(Duration::from_secs(10), cl.call(&cmd)).await {
            Ok(Ok(r)) => {
                rec.finish(op, sh.elapsed(), r.clone());
                Some(r)
            }
            _ => None,
        }
    };
    for cmd in cmds {
        if call(&mut cl, cmd).await.is_none() {
            ok = false;
            break;
        }
    }
    while ok {
        match call(&mut cl, Cmd::ReserveWithTimeout(0)).await {
            Some(Reply::Reserved { id, .. }) => {
                if call(&mut cl, Cmd::Delete(id)).await.is_none() {
                    ok = false;
                }
            }
            Some(_) => break,
            None => ok = false,
        }
    }
    drop(cl);
    rec.close_conn(key, sh.elapsed());
    if !ok {
        sh.problem("liveness: a verification command got no reply within 10 s".into());
    }
}

/// Replicated `stats` fields, which every node must agree on.
const REPLICATED: &[&str] = &[
    "current-jobs-urgent",
    "current-jobs-ready",
    "current-jobs-reserved",
    "current-jobs-delayed",
    "current-jobs-buried",
    "cmd-put",
    "cmd-delete",
    "cmd-reserve-with-timeout",
    "cmd-release",
    "cmd-bury",
    "cmd-kick",
    "cmd-touch",
    "job-timeouts",
    "total-jobs",
    "current-tubes",
];

async fn compare_replicas(c: &Cluster, sh: &Shared) {
    // Wait until every node applied the same index.
    let ids: Vec<u64> = c.nodes.iter().map(|n| n.id).collect();
    let same = wait_for(Duration::from_secs(20), || async {
        let mut idx = BTreeSet::new();
        for &id in &ids {
            match c.admin(id).await {
                Some(a) => {
                    idx.insert(a["cluster"]["applied_index"].as_u64());
                }
                None => return false,
            }
        }
        idx.len() == 1
    })
    .await;
    if !same {
        sh.problem("replicas did not converge on one applied index within 20 s".into());
        return;
    }
    let mut seen: Option<(u64, BTreeMap<String, String>)> = None;
    for n in &c.nodes {
        let Ok(mut cl) = BsClient::connect(n.client, Duration::from_secs(2)).await else {
            sh.problem(format!("stats: cannot connect to node {}", n.id));
            return;
        };
        let Ok(Ok((line, body))) =
            tokio::time::timeout(Duration::from_secs(10), cl.raw("stats")).await
        else {
            sh.problem(format!("stats: no reply from node {}", n.id));
            return;
        };
        if !line.starts_with("OK") {
            sh.problem(format!("stats on node {}: {line}", n.id));
            return;
        }
        let text = String::from_utf8_lossy(&body);
        let fields: BTreeMap<String, String> = text
            .lines()
            .filter_map(|l| l.split_once(": "))
            .filter(|(k, _)| REPLICATED.contains(k))
            .map(|(k, v)| (k.to_string(), v.trim().to_string()))
            .collect();
        match &seen {
            None => seen = Some((n.id, fields.clone())),
            Some((first, want)) => {
                for (k, v) in want {
                    if fields.get(k) != Some(v) {
                        sh.problem(format!(
                            "replicated stats differ: node {first} {k}={v}, node {} {k}={:?}",
                            n.id,
                            fields.get(k)
                        ));
                    }
                }
            }
        }
        let jobs: u64 = ["urgent", "ready", "reserved", "delayed", "buried"]
            .iter()
            .filter(|s| **s != "urgent")
            .filter_map(|s| {
                fields
                    .get(&format!("current-jobs-{s}"))?
                    .parse::<u64>()
                    .ok()
            })
            .sum();
        if jobs != 0 {
            sh.problem(format!(
                "node {} still has {jobs} jobs after the drain",
                n.id
            ));
        }
    }
}

/// Checks that Raft RPCs and forwards use each node's configured peer
/// addresses (the proxies of the directed links), not the membership
/// addresses openraft stores (those of node 1's configuration, which
/// bootstrapped the cluster): with node 1 killed, the new leader `L`
/// replicates to the follower `F` through proxy `L → F`, and `F` forwards
/// a client's puts through proxy `F → L`, while the proxies `1 → x` (the
/// membership addresses) carry nothing.
pub async fn check_peer_addresses() -> Result<String, String> {
    let mut cfg = MpConfig::from_seed(0, Duration::ZERO);
    cfg.base_dir = None;
    let sh = Shared {
        t0: Instant::now(),
        events: Mutex::new(Vec::new()),
        problems: Mutex::new(Vec::new()),
        faults: Mutex::new(BTreeMap::new()),
    };
    let mut c = start_cluster(&cfg, 0, &sh).await?;
    kill(&mut c, 1, &sh);
    let deadline = Instant::now() + Duration::from_secs(20);
    let l = loop {
        if let Some(x) = c.leader().await
            && x != 1
            && c.ready(x).await
        {
            break x;
        }
        if Instant::now() >= deadline {
            return Err("no new leader after killing node 1".into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let f = if l == 2 { 3 } else { 2 };
    let fref = &c;
    if !wait_for(
        Duration::from_secs(20),
        || async move { fref.ready(f).await },
    )
    .await
    {
        return Err(format!("follower {f} not ready"));
    }
    let before: BTreeMap<(u64, u64), (u64, u64)> =
        c.proxies.iter().map(|(k, p)| (*k, p.bytes())).collect();
    let mut cl = BsClient::connect(c.nodes[(f - 1) as usize].client, Duration::from_secs(2))
        .await
        .map_err(|e| e.to_string())?;
    for i in 0..20 {
        let cmd = Cmd::Put {
            pri: 0,
            delay: 0,
            ttr: 10,
            body: format!("addr-check-{i}").into_bytes(),
        };
        let r = tokio::time::timeout(Duration::from_secs(5), cl.call(&cmd))
            .await
            .map_err(|_| "put timed out".to_string())?
            .map_err(|e| e.to_string())?;
        if !matches!(r, Reply::Inserted(_)) {
            return Err(format!("put via node {f}: {r:?}"));
        }
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let delta = |k: (u64, u64)| {
        let (u0, d0) = before.get(&k).copied().unwrap_or_default();
        let (u1, d1) = c.proxies.get(&k).map(Proxy::bytes).unwrap_or_default();
        (u1 - u0, d1 - d0)
    };
    let replication = delta((l, f));
    let forwards = delta((f, l));
    let membership: Vec<((u64, u64), (u64, u64))> = [(1, l), (1, f)]
        .into_iter()
        .map(|k| (k, delta(k)))
        .collect();
    let report = format!(
        "leader {l}, follower {f}: replication {l}->{f} {replication:?} bytes (up, down), \
         forwards {f}->{l} {forwards:?}, membership-address links {membership:?}"
    );
    if replication.0 == 0 || replication.1 == 0 || forwards.0 == 0 || forwards.1 == 0 {
        return Err(format!("configured peer addresses not used: {report}"));
    }
    if membership.iter().any(|(_, (u, d))| *u + *d > 0) {
        return Err(format!("traffic on the membership addresses: {report}"));
    }
    Ok(report)
}

/// Finding probe: a client reconnecting in a tight loop to a node cut off
/// from the cluster. Every connection the isolated node accepts (and closes
/// at once) still queues a `Connect` and a `Disconnect` for the leader, so
/// the node's forward queue grows without bound while it is isolated and
/// floods the log once it is back. Returns what was observed.
pub async fn reconnect_storm_probe(isolate: Duration) -> Result<String, String> {
    let mut cfg = MpConfig::from_seed(0, Duration::ZERO);
    cfg.base_dir = None;
    let sh = Shared {
        t0: Instant::now(),
        events: Mutex::new(Vec::new()),
        problems: Mutex::new(Vec::new()),
        faults: Mutex::new(BTreeMap::new()),
    };
    let c = start_cluster(&cfg, 0, &sh).await?;
    let l = c.leader().await.ok_or("no leader")?;
    let x = if l == 1 { 2 } else { 1 };
    let log0 = c
        .admin(l)
        .await
        .and_then(|a| a["cluster"]["last_log_index"].as_u64());
    for p in c.links_of(x) {
        p.sever();
    }
    let addr = c.nodes[(x - 1) as usize].client;
    let until = Instant::now() + isolate;
    let mut conns = 0u64;
    while Instant::now() < until {
        if let Ok(mut cl) = BsClient::connect(addr, Duration::from_millis(200)).await {
            conns += 1;
            let _ = tokio::time::timeout(Duration::from_millis(200), cl.raw("stats-tube default"))
                .await;
        }
    }
    let queued = c
        .admin(x)
        .await
        .and_then(|a| a["cluster"]["forward_queue"].as_u64());
    for p in c.proxies.values() {
        p.heal(false);
    }
    let healed = Instant::now();
    let ok = wait_for(Duration::from_secs(120), || async {
        let mut all = true;
        for id in 1..=3 {
            let q = c
                .admin(id)
                .await
                .and_then(|a| a["cluster"]["forward_queue"].as_u64());
            all &= q == Some(0) && c.ready(id).await;
        }
        all
    })
    .await;
    let log1 = c
        .admin(l)
        .await
        .and_then(|a| a["cluster"]["last_log_index"].as_u64());
    let report = format!(
        "node {x} isolated for {isolate:?}: {conns} connections accepted and closed; its forward \
         queue held {queued:?} items; after healing, the leader's log went from {log0:?} to \
         {log1:?} and the cluster {} in {:.1?}",
        if ok {
            "drained its queues"
        } else {
            "had NOT drained its queues"
        },
        healed.elapsed()
    );
    Ok(report)
}
