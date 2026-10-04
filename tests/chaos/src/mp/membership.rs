//! Membership scenarios of the multi-process harness (P6-T7), driven
//! through the real operator command (`beanstalkd-rs cluster
//! --insecure-plaintext`, from loopback, against the nodes' own cluster
//! ports: the operator's path does not cross the per-link proxies).
//!
//! Every node's configuration lists the spare ids from the start, each
//! behind its own per-link proxy, so a node dials a new member through the
//! proxy (a config address is a local override of the membership's,
//! docs/DESIGN.md §8); a spare is added at its real cluster address and
//! started without `--cluster-init` (it joins).
//!
//! The scenarios ([`Step`]) run in order, at evenly spaced times, while the
//! random faults of the fixed mix go on (their node ids follow the live
//! nodes):
//!
//! 1. add a spare while a follower is cut off, start it, heal, promote it;
//! 2. remove a follower whose client holds a reservation (a job with a long
//!    TTR in a tube of its own), stop it, and time how long until the job
//!    is ready again;
//! 3. add a spare and promote it, killing the leader as soon as its
//!    `/admin` shows the joint configuration (or, if the joint step was
//!    not seen, shortly after the promote was sent; the binary is built
//!    without the `test-hooks` feature, so `test-hold-change` is not
//!    available and the timing is best effort), then restart it;
//! 4. remove the leader, stop it;
//! 5. try to add a removed id again (must be refused: ids are never reused);
//! 6. replace a node's disk: kill it, wipe its data, restart it (it rejoins),
//!    and change its membership address;
//! 7. restart a node with a stale configuration (only the initial voters
//!    listed): the log's membership wins, and it reaches the new members at
//!    their membership addresses.
//!
//! A step that a guardrail refuses because of the random faults (a voter
//! down, a learner catching up) is retried for a while, as an operator
//! would; what was asked and answered is recorded either way.
//!
//! Invariants after heal and settle: the leader's membership is committed
//! and uniform, every member reports the same one, and it is one of the
//! memberships the answered changes allow (a change the command could not
//! confirm may or may not have happened; a refused one did not); no
//! connection is left in the replicated state once every client has gone
//! (so every owner is a member: a removed node's connections were closed);
//! the removed node's reservation was released within [`RELEASE_BOUND`] of
//! the later of its removal and the last fault before; the highest member
//! id never decreased and a removed id was refused. The rejoin decisions'
//! inputs (invariant 6 of the in-process harness) are not observable here.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use bstk_raft::sim::SimRng;
use nix::sys::signal::Signal;
use serde_json::Value;

use super::{Cluster, MpFault, Shared, kill, lock, restart, wait_for, wipe_allowed};
use crate::client::BsClient;
use crate::history::Cmd;

/// `2 × node_timeout` (1 s here) plus a margin.
pub const RELEASE_BOUND: Duration = Duration::from_secs(5);
/// How long a step retries a change that a guardrail refuses for now.
const RETRY_BUDGET: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq)]
pub enum Step {
    GrowDuringPartition(u64),
    RemoveHolding(u64),
    GrowKillLeaderMidChange(u64),
    RemoveLeader(u64),
    ReuseRemovedId,
    ReplaceWiped(u64),
    StaleConfigRestart(u64),
}

/// The scenarios, evenly spread over the run.
pub fn schedule(seed: u64, duration: Duration) -> Vec<(Duration, MpFault)> {
    let mut r = SimRng::new(seed ^ 0x3E3B_0000);
    let steps = [
        Step::GrowDuringPartition(r.next_u64()),
        Step::RemoveHolding(r.next_u64()),
        Step::GrowKillLeaderMidChange(r.next_u64()),
        Step::RemoveLeader(r.next_u64()),
        Step::ReuseRemovedId,
        Step::ReplaceWiped(r.next_u64()),
        Step::StaleConfigRestart(r.next_u64()),
    ];
    let n = steps.len() as u32 + 1;
    steps
        .into_iter()
        .enumerate()
        .map(|(i, s)| (duration * (i as u32 + 1) / n, MpFault::Member(s)))
        .collect()
}

/// One node of the membership model: its address and whether it votes.
type Model = BTreeMap<u64, (String, bool)>;

#[derive(Debug, Clone)]
enum Change {
    Add(u64, String),
    Promote(u64),
    Remove(u64),
    SetAddr(u64, String),
}

fn change(m: &Model, c: &Change) -> Model {
    let mut m = m.clone();
    match c {
        Change::Add(id, addr) => {
            m.entry(*id).or_insert((addr.clone(), false));
        }
        Change::Promote(id) => {
            if let Some(n) = m.get_mut(id) {
                n.1 = true;
            }
        }
        Change::Remove(id) => {
            m.remove(id);
        }
        Change::SetAddr(id, addr) => {
            if let Some(n) = m.get_mut(id) {
                n.0 = addr.clone();
            }
        }
    }
    m
}

/// The memberships the answered changes allow.
pub(super) struct Models(Vec<Model>);

impl Models {
    /// Whether some allowed membership lists `id`.
    fn may_have(&self, id: u64) -> bool {
        self.0.iter().any(|m| m.contains_key(&id))
    }

    fn apply(&mut self, c: &Change, code: Option<i32>) {
        let mut next: Vec<Model> = Vec::new();
        for m in &self.0 {
            match code {
                Some(0) => next.push(change(m, c)),
                Some(1) => next.push(m.clone()),
                // Not confirmed: either.
                _ => {
                    next.push(m.clone());
                    next.push(change(m, c));
                }
            }
        }
        next.sort();
        next.dedup();
        // Bounded: a run sends a handful of changes.
        next.truncate(256);
        self.0 = next;
    }
}

/// The node's cluster address as `127.0.0.1:port`.
fn real_addr(c: &Cluster, id: u64) -> String {
    c.nodes[(id - 1) as usize].cluster.to_string()
}

/// The initial membership, as the cluster reports it after its addresses
/// were set to the nodes' own listeners. Whichever initial voter
/// bootstrapped it wrote its own config's addresses, the per-link proxies of
/// its links; the CLI follows the membership's address of the leader, so a
/// cut link would hide the leader from the operator (each node's config
/// still overrides these addresses, so its traffic stays on the proxies).
pub(super) async fn init_models(c: &mut Cluster, sh: &Shared) {
    for id in 1..=c.initial {
        let addr = real_addr(c, id);
        until_done(c, sh, &Change::SetAddr(id, addr)).await;
    }
    let m = wait_for(Duration::from_secs(10), || async {
        membership(c).await.is_some_and(|m| m["committed"] == true)
    })
    .await;
    let Some(v) = membership(c).await.filter(|_| m) else {
        sh.problem("no initial membership from the leader".into());
        return;
    };
    *lock(&sh.models) = Some(Models(vec![model_of(&v)]));
}

/// `m` with every address some node's config gives the node (its own
/// listener, or a proxy in front of it) written as `config`. Each initial
/// voter's config lists the others at its own per-link proxies, so the
/// bootstrap entry (the same log id on every initial voter) carries each
/// node's own spelling, and a later change on another leader rewrites it:
/// the docs require the same peer list on every initial voter, which these
/// configs cannot have. Addresses the operator set stay as they are.
fn canon(c: &Cluster, m: &Model) -> Model {
    m.iter()
        .map(|(&id, (addr, voter))| {
            let configured = real_addr(c, id) == *addr
                || c.proxies
                    .iter()
                    .any(|(&(_, b), p)| b == id && p.addr().to_string() == *addr);
            let addr = if configured {
                "config".to_string()
            } else {
                addr.clone()
            };
            (id, (addr, *voter))
        })
        .collect()
}

/// A `/admin` membership object as a model.
fn model_of(m: &Value) -> Model {
    let voters = ids_of(&m["voters"]);
    m["nodes"]
        .as_object()
        .map(|o| {
            o.iter()
                .filter_map(|(k, v)| {
                    let id: u64 = k.parse().ok()?;
                    Some((id, (v.as_str()?.to_string(), voters.contains(&id))))
                })
                .collect()
        })
        .unwrap_or_default()
}

struct CliOut {
    code: Option<i32>,
    out: String,
    err: String,
}

/// Runs `beanstalkd-rs cluster ARGS` against every live node's cluster port
/// (in order, as seeds).
async fn cli(c: &Cluster, args: &[String]) -> CliOut {
    let mut cmd = tokio::process::Command::new(&c.bin);
    cmd.arg("cluster")
        .args(args)
        .args(["--insecure-plaintext", "--json", "--timeout", "20s"]);
    for id in c.live_ids() {
        if c.nodes[(id - 1) as usize].starts > 0 {
            cmd.arg("--node").arg(real_addr(c, id));
        }
    }
    cmd.stdin(std::process::Stdio::null()).kill_on_drop(true);
    match cmd.output().await {
        Ok(o) => CliOut {
            code: o.status.code(),
            out: String::from_utf8_lossy(&o.stdout).into_owned(),
            err: String::from_utf8_lossy(&o.stderr).into_owned(),
        },
        Err(e) => CliOut {
            code: None,
            out: String::new(),
            err: format!("spawn: {e}"),
        },
    }
}

/// Sends `c` once; records it in the models and the events.
async fn send(cl: &mut Cluster, sh: &Shared, ch: &Change) -> CliOut {
    let args: Vec<String> = match ch {
        Change::Add(id, addr) => vec!["add".into(), id.to_string(), addr.clone()],
        Change::Promote(id) => vec!["promote".into(), id.to_string()],
        Change::Remove(id) => vec!["remove".into(), id.to_string()],
        Change::SetAddr(id, addr) => vec!["set-addr".into(), id.to_string(), addr.clone()],
    };
    let o = cli(cl, &args).await;
    let why = if o.code == Some(0) {
        String::new()
    } else {
        let text = if o.err.is_empty() { &o.out } else { &o.err };
        text.chars().take(200).collect()
    };
    sh.event(format!(
        "cluster {}: exit {:?} {why}",
        args.join(" "),
        o.code
    ));
    sh.count(&format!(
        "cli:{}:{}",
        args[0],
        o.code.map_or("signal".to_string(), |c| c.to_string())
    ));
    if let Some(m) = lock(&sh.models).as_mut() {
        m.apply(ch, o.code);
    }
    o
}

/// Refusals that clear by themselves: retry them.
fn transient(o: &CliOut) -> bool {
    let text = format!("{}{}", o.out, o.err);
    o.code != Some(0) && o.code != Some(1) && o.code != Some(2)
        || [
            "not caught up",
            "has not replicated",
            "did not answer",
            "in progress",
            "rejoining",
            "conflict",
            "joint",
        ]
        .iter()
        .any(|k| text.contains(k))
}

/// Sends `ch` until it succeeds, is refused for good, or the budget runs
/// out.
async fn until_done(cl: &mut Cluster, sh: &Shared, ch: &Change) -> bool {
    let deadline = Instant::now() + RETRY_BUDGET;
    loop {
        let o = send(cl, sh, ch).await;
        if o.code == Some(0) {
            return true;
        }
        if !transient(&o) || Instant::now() >= deadline {
            sh.count("StepGaveUp");
            return false;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// The leader's `/admin` membership object.
async fn membership(c: &Cluster) -> Option<Value> {
    let l = c.leader().await?;
    let a = c.admin(l).await?;
    Some(a["cluster"]["membership"].clone())
}

fn ids_of(v: &Value) -> BTreeSet<u64> {
    v.as_array()
        .map(|a| a.iter().filter_map(Value::as_u64).collect())
        .unwrap_or_default()
}

async fn voters(c: &Cluster) -> BTreeSet<u64> {
    membership(c)
        .await
        .map(|m| ids_of(&m["voters"]))
        .unwrap_or_default()
}

fn pick(v: &[u64], draw: u64) -> Option<u64> {
    (!v.is_empty()).then(|| v[(draw % v.len() as u64) as usize])
}

/// The next spare that never ran.
fn spare(c: &Cluster) -> Option<u64> {
    c.nodes
        .iter()
        .find(|n| n.id > c.initial && n.starts == 0 && !n.retired)
        .map(|n| n.id)
}

/// Kills node `id` and never starts it again (the runbook's "stop its
/// process" after a removal).
fn retire(c: &mut Cluster, id: u64, sh: &Shared) {
    kill(c, id, sh);
    if let Some(n) = c.node(id) {
        n.retired = true;
    }
    sh.event(format!("operator stops removed node {id}"));
}

fn start_new(c: &mut Cluster, id: u64, sh: &Shared) {
    let bin = c.bin.clone();
    if let Some(n) = c.node(id) {
        match n.start(&bin, false) {
            Ok(()) => sh.event(format!("start node {id} (joins)")),
            Err(e) => sh.problem(e),
        }
    }
}

async fn followers(c: &Cluster) -> Vec<u64> {
    let l = c.leader().await;
    voters(c)
        .await
        .into_iter()
        .filter(|&v| Some(v) != l && c.nodes[(v - 1) as usize].child.is_some())
        .collect()
}

pub(super) async fn run_step(c: &mut Cluster, step: &Step, sh: &Shared) {
    sh.event(format!("membership scenario {step:?}"));
    // An operator brings dead or stopped nodes back before changing the
    // membership (a dead voter blocks every voter change but its removal).
    let bin = c.bin.clone();
    for id in c.started_ids() {
        if let Some(n) = c.node(id)
            && n.stopped
        {
            n.signal(Signal::SIGCONT);
        }
        restart(c, &bin, id, false, sh);
    }
    let name = format!("{step:?}");
    sh.count(name.split('(').next().unwrap_or_default());
    match *step {
        Step::GrowDuringPartition(draw) => {
            let Some(s) = spare(c) else { return };
            let cut = pick(&followers(c).await, draw);
            if let Some(f) = cut {
                for p in c.links_of(f) {
                    p.sever();
                }
                sh.event(format!("isolate node {f} while adding node {s}"));
            }
            let added = until_done(c, sh, &Change::Add(s, real_addr(c, s))).await;
            if added || lock(&sh.models).as_ref().is_some_and(|m| m.may_have(s)) {
                start_new(c, s, sh);
            }
            if let Some(f) = cut {
                tokio::time::sleep(Duration::from_millis(500 + draw % 1500)).await;
                for p in c.links_of(f) {
                    p.heal(false);
                }
                sh.event(format!("heal node {f}'s links"));
            }
            if added && until_done(c, sh, &Change::Promote(s)).await {
                sh.count("GrewDuringPartition");
            }
        }
        Step::RemoveHolding(draw) => remove_holding(c, sh, draw).await,
        Step::GrowKillLeaderMidChange(draw) => {
            let Some(s) = spare(c) else { return };
            if !until_done(c, sh, &Change::Add(s, real_addr(c, s))).await {
                return;
            }
            start_new(c, s, sh);
            kill_mid_change(c, sh, s, draw).await;
        }
        Step::RemoveLeader(draw) => {
            let Some(l) = c.leader().await else {
                return sh.count("RemoveLeaderSkipped");
            };
            if voters(c).await.len() < 4 {
                // Would need force; the scenario keeps three voters.
                return sh.count("RemoveLeaderSkipped");
            }
            if until_done(c, sh, &Change::Remove(l)).await {
                sh.count("RemovedLeader");
                tokio::time::sleep(Duration::from_millis(draw % 2000)).await;
                retire(c, l, sh);
            }
        }
        Step::ReuseRemovedId => {
            let Some(r) = c.nodes.iter().find(|n| n.retired).map(|n| n.id) else {
                return sh.count("ReuseSkipped");
            };
            let addr = real_addr(c, r);
            let o = cli(c, &["add".into(), r.to_string(), addr]).await;
            sh.event(format!(
                "cluster add {r} (a removed id): exit {:?} {}",
                o.code,
                o.err.chars().take(160).collect::<String>()
            ));
            if o.code == Some(0) {
                sh.problem(format!(
                    "invariant 5: removed node id {r} was added again: {}",
                    o.out
                ));
            } else if o.code == Some(1)
                && !o.err.contains("never reused")
                && !o.out.contains("never reused")
            {
                sh.event(format!("add {r} refused for another reason"));
            } else {
                sh.count("ReuseRefused");
            }
        }
        Step::ReplaceWiped(draw) => {
            let Some(y) = pick(&followers(c).await, draw) else {
                return sh.count("ReplaceWipedSkipped");
            };
            if let Err(why) = wipe_allowed(c, y).await {
                sh.event(format!("replace of node {y}'s disk skipped ({why})"));
                return sh.count("ReplaceWipedSkipped");
            }
            let bin = c.bin.clone();
            kill(c, y, sh);
            restart(c, &bin, y, true, sh);
            // A new address: the other spelling of its loopback port.
            let port = c.nodes[(y - 1) as usize].cluster.port();
            let cur = membership(c)
                .await
                .and_then(|m| m["nodes"][y.to_string()].as_str().map(str::to_string));
            let addr = if cur.as_deref().is_some_and(|a| a.starts_with("localhost")) {
                format!("127.0.0.1:{port}")
            } else {
                format!("localhost:{port}")
            };
            if until_done(c, sh, &Change::SetAddr(y, addr)).await {
                sh.count("ReplacedWiped");
            }
        }
        Step::StaleConfigRestart(draw) => {
            let Some(z) = pick(&followers(c).await, draw) else {
                return sh.count("StaleRestartSkipped");
            };
            if let Err(e) = write_stale_config(c, z) {
                return sh.problem(e);
            }
            let bin = c.bin.clone();
            kill(c, z, sh);
            restart(c, &bin, z, false, sh);
            sh.event(format!(
                "node {z} restarted with a stale configuration (initial voters only)"
            ));
            sh.count("StaleRestarted");
        }
    }
}

/// Rewrites node `z`'s configuration to list only the initial voters (as
/// before any membership change).
fn write_stale_config(c: &Cluster, z: u64) -> Result<(), String> {
    let n = &c.nodes[(z - 1) as usize];
    let text = std::fs::read_to_string(&n.config).map_err(|e| e.to_string())?;
    let mut out = String::new();
    let mut skip = false;
    for block in text.split_inclusive('\n') {
        if block.starts_with("[[cluster.peer]]") {
            skip = false;
        }
        if let Some(id) = block.strip_prefix("id = ")
            && id.trim().parse::<u64>().is_ok_and(|id| id > c.initial)
        {
            // Drop this peer entry (its header was already written).
            skip = true;
            if out.ends_with("[[cluster.peer]]\n") {
                out.truncate(out.len() - "[[cluster.peer]]\n".len());
            }
            continue;
        }
        if skip && block.starts_with("addr = ") {
            skip = false;
            continue;
        }
        out.push_str(block);
    }
    std::fs::write(&n.config, out).map_err(|e| e.to_string())
}

/// Scenario 2 (see the module docs).
/// Deletes probe job `job` once no connection holds it (a reserved job can
/// only be deleted by its holder), for up to 60 s.
async fn delete_probe(c: &Cluster, sh: &Shared, job: u64) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if let Some(l) = c.leader().await
            && let Ok(mut cl) =
                BsClient::connect(c.nodes[(l - 1) as usize].client, Duration::from_secs(1)).await
            && let Ok(Ok((line, body))) =
                tokio::time::timeout(Duration::from_secs(2), cl.raw(&format!("stats-job {job}")))
                    .await
        {
            if line.starts_with("NOT_FOUND") {
                return;
            }
            if line.starts_with("OK") && !String::from_utf8_lossy(&body).contains("state: reserved")
            {
                let _ =
                    tokio::time::timeout(Duration::from_secs(2), cl.raw(&format!("delete {job}")))
                        .await;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    sh.problem(format!("probe job {job} could not be deleted within 60 s"));
}

async fn remove_holding(c: &mut Cluster, sh: &Shared, draw: u64) {
    let fs = followers(c).await;
    let Some(x) = pick(&fs, draw) else {
        return sh.count("RemoveHoldingSkipped");
    };
    if voters(c).await.len() < 4 {
        return sh.count("RemoveHoldingSkipped");
    }
    let addr = c.nodes[(x - 1) as usize].client;
    let body = format!("membership-probe-{draw}");
    let held = async {
        let mut cl = BsClient::connect(addr, Duration::from_secs(1)).await.ok()?;
        let t = Duration::from_secs(5);
        let r = async |cl: &mut BsClient, l: &str| {
            tokio::time::timeout(t, cl.raw(l))
                .await
                .ok()
                .and_then(Result::ok)
        };
        r(&mut cl, "use probe").await?;
        let put = Cmd::Put {
            pri: 0,
            delay: 0,
            ttr: 600,
            body: body.clone().into_bytes(),
        };
        let id = match tokio::time::timeout(t, cl.call(&put)).await.ok()?.ok()? {
            crate::history::Reply::Inserted(id) => id,
            _ => return None,
        };
        r(&mut cl, "watch probe").await?;
        r(&mut cl, "ignore default").await?;
        match tokio::time::timeout(t, cl.call(&Cmd::ReserveWithTimeout(2)))
            .await
            .ok()?
            .ok()?
        {
            crate::history::Reply::Reserved { id: got, .. } if got == id => Some((cl, id)),
            _ => None,
        }
    }
    .await;
    let Some((holder, job)) = held else {
        return sh.count("RemoveHoldingNoReservation");
    };
    sh.event(format!("node {x}'s client holds probe job {job} reserved"));
    if !until_done(c, sh, &Change::Remove(x)).await {
        // The final drain checks that no job is left; it does not watch
        // the probe tube.
        let mut holder = holder;
        let deleted =
            tokio::time::timeout(Duration::from_secs(5), holder.raw(&format!("delete {job}")))
                .await
                .ok()
                .and_then(Result::ok)
                .is_some_and(|(line, _)| line.starts_with("DELETED"));
        drop(holder);
        if !deleted {
            delete_probe(c, sh, job).await;
        }
        return;
    }
    let removed = sh.elapsed();
    sh.count("RemovedNodeHoldingReservation");
    tokio::time::sleep(Duration::from_millis(draw % 2000)).await;
    retire(c, x, sh);
    drop(holder);
    // Until the job is ready again (only the removal can release it: its TTR
    // is 10 minutes), from the leader.
    let mut released = None;
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline && released.is_none() {
        if let Some(l) = c.leader().await
            && let Ok(mut cl) =
                BsClient::connect(c.nodes[(l - 1) as usize].client, Duration::from_secs(1)).await
            && let Ok(Ok((line, body))) =
                tokio::time::timeout(Duration::from_secs(2), cl.raw(&format!("stats-job {job}")))
                    .await
            && line.starts_with("OK")
            && String::from_utf8_lossy(&body).contains("state: ready")
        {
            released = Some(sh.elapsed());
            let _ = tokio::time::timeout(Duration::from_secs(2), cl.raw(&format!("delete {job}")))
                .await;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let Some(released) = released else {
        sh.problem(format!(
            "invariant 3: node {x} was removed at {removed:?}, but its client's reservation of \
             job {job} was not released within 60 s"
        ));
        return;
    };
    delete_probe(c, sh, job).await;
    let anchor = lock(&sh.disruptions)
        .iter()
        .copied()
        .filter(|&t| t <= released)
        .fold(removed, Duration::max);
    sh.event(format!(
        "probe job {job} ready again {:?} after node {x}'s removal",
        released.saturating_sub(removed)
    ));
    if released > anchor + RELEASE_BOUND {
        sh.problem(format!(
            "invariant 3: node {x} was removed at {removed:?}, but its client's reservation of \
             job {job} was released only at {released:?} (more than {RELEASE_BOUND:?} after \
             {anchor:?})"
        ));
    }
}

/// Scenario 3's promote with the leader killed between the joint and the
/// uniform step (see the module docs).
async fn kill_mid_change(c: &mut Cluster, sh: &Shared, s: u64, draw: u64) {
    // Let the learner catch up first, so the promote is not refused.
    let caught_up = wait_for(Duration::from_secs(20), || async {
        let Some(l) = c.leader().await else {
            return false;
        };
        let Some(a) = c.admin(l).await else {
            return false;
        };
        a["cluster"]["learner_lag"][s.to_string()]
            .as_u64()
            .is_some_and(|lag| lag < 50)
    })
    .await;
    if !caught_up {
        sh.count("KillMidChangeSkipped");
        return;
    }
    let Some(l) = c.leader().await else {
        return;
    };
    let mut cmd = tokio::process::Command::new(&c.bin);
    cmd.args(["cluster", "promote", &s.to_string()])
        .args(["--insecure-plaintext", "--json", "--timeout", "20s"])
        .arg("--node")
        .arg(real_addr(c, l))
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    let Ok(child) = cmd.spawn() else {
        return sh.problem("cannot spawn the cluster command".into());
    };
    let started = Instant::now();
    let blind = Duration::from_millis(30 + draw % 200);
    let mut seen = false;
    while started.elapsed() < blind {
        if let Some(a) = c.admin(l).await
            && a["cluster"]["membership"]["joint"].as_bool() == Some(true)
        {
            seen = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    lock(&sh.disruptions).push(sh.elapsed());
    kill(c, l, sh);
    sh.event(format!(
        "leader {l} killed {} the promote of node {s}",
        if seen {
            "between the joint and the uniform step of"
        } else {
            "shortly after"
        }
    ));
    sh.count(if seen {
        "KilledLeaderInJointStep"
    } else {
        "KilledLeaderAfterPromote"
    });
    let o = child.wait_with_output().await;
    let code = o.as_ref().ok().and_then(|o| o.status.code());
    sh.event(format!(
        "cluster promote {s} (leader killed): exit {code:?}"
    ));
    if let Some(m) = lock(&sh.models).as_mut() {
        m.apply(&Change::Promote(s), code);
    }
    tokio::time::sleep(Duration::from_millis(1000 + draw % 1000)).await;
    let bin = c.bin.clone();
    restart(c, &bin, l, false, sh);
}

/// After the heal: the membership the leader reports once it is committed,
/// uniform and the same twice a second apart; every other node is stopped,
/// every member started. `None` on a failure (recorded).
pub(super) async fn settle(c: &mut Cluster, sh: &Shared) -> Option<BTreeSet<u64>> {
    let mut last: Option<Value> = None;
    let mut settled = None;
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if let Some(m) = membership(c).await
            && m["committed"].as_bool() == Some(true)
            && m["joint"].as_bool() == Some(false)
        {
            if last.as_ref() == Some(&m) {
                settled = Some(m);
                break;
            }
            last = Some(m);
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let Some(m) = settled else {
        sh.problem(format!(
            "invariant 1: no committed, uniform membership 60 s after healing (last seen {last:?})"
        ));
        return None;
    };
    let mut members = ids_of(&m["voters"]);
    members.extend(ids_of(&m["learners"]));
    sh.event(format!("settled membership: {m}"));
    let ids: Vec<u64> = c.nodes.iter().map(|n| n.id).collect();
    for id in ids {
        let running = c.nodes[(id - 1) as usize].child.is_some();
        if !members.contains(&id) && running {
            retire(c, id, sh);
        }
        if members.contains(&id) {
            if let Some(n) = c.node(id)
                && n.stopped
            {
                n.signal(Signal::SIGCONT);
            }
            if c.nodes[(id - 1) as usize].retired {
                sh.problem(format!(
                    "node {id} is a member, but it was stopped as removed"
                ));
            }
            let bin = c.bin.clone();
            restart(c, &bin, id, false, sh);
        }
    }
    // The model: is this one of the memberships the answers allow?
    let got = canon(c, &model_of(&m));
    if let Some(models) = lock(&sh.models).take()
        && !models.0.iter().any(|w| canon(c, w) == got)
    {
        sh.problem(format!(
            "invariant 1: the final membership {got:?} is none of those the operator's answered \
             changes allow: {:?}",
            models.0
        ));
    }
    Some(members)
}

/// Invariants 1 (every member reports the settled membership), 2 (no
/// connection left once the clients are gone) and 5 (the highest member
/// id), after the final verification.
pub(super) async fn check_final(c: &Cluster, sh: &Shared, members: &[u64]) {
    let Some(m) = membership(c).await else {
        sh.problem("invariant 1: no membership from the leader at the end".into());
        return;
    };
    let agree = wait_for(Duration::from_secs(10), || async {
        for &id in members {
            let Some(a) = c.admin(id).await else {
                return false;
            };
            let v = &a["cluster"]["membership"];
            if v["log_index"] != m["log_index"] || v["joint"] != m["joint"] {
                return false;
            }
        }
        true
    })
    .await;
    if !agree {
        sh.problem(format!(
            "invariant 1: the members do not all report the leader's membership {m}"
        ));
    }
    let highest = m["highest_member"].as_u64().unwrap_or(0);
    let max_ever = c
        .nodes
        .iter()
        .filter(|n| n.starts > 0)
        .map(|n| n.id)
        .max()
        .unwrap_or(0);
    if highest < max_ever.min(members.iter().copied().max().unwrap_or(0)) {
        sh.problem(format!(
            "invariant 5: the highest member id {highest} is below a member's id"
        ));
    }
    // Every client is gone: the replicated state holds no connection but
    // the one asking (a removed or restarted node's are closed by its
    // `DropNode`).
    let Some(l) = c.leader().await else {
        return;
    };
    let addr = c.nodes[(l - 1) as usize].client;
    let seen = std::sync::Mutex::new(None);
    let seen = &seen;
    let none_left = wait_for(Duration::from_secs(10), || async move {
        let Ok(mut cl) = BsClient::connect(addr, Duration::from_secs(1)).await else {
            return false;
        };
        let Ok(Ok((_, body))) = tokio::time::timeout(Duration::from_secs(2), cl.raw("stats")).await
        else {
            return false;
        };
        let text = String::from_utf8_lossy(&body).into_owned();
        let n = text
            .lines()
            .find_map(|l| l.strip_prefix("current-connections: "))
            .and_then(|v| v.trim().parse::<u64>().ok());
        *lock(seen) = n;
        n == Some(1)
    })
    .await;
    if !none_left {
        sh.problem(format!(
            "invariant 2: connections left in the replicated state after every client left: \
             current-connections {:?} (expected 1, the asking one)",
            lock(seen)
        ));
    }
}
